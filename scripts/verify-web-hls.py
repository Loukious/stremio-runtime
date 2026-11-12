#!/usr/bin/env python3
"""Verify a real runtime against local media, then optionally serve the web-player fixture.

Requires Python 3.9+, FFmpeg and FFprobe. Uses task-owned data and an ephemeral
HTTP port. No installed Stremio process, debrid account, or real media is used.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import urlopen
from urllib.error import HTTPError


def request(url, expected=200):
    try:
        with urlopen(url, timeout=120) as response:
            status, headers, body = response.status, response.headers, response.read()
    except HTTPError as error:
        status, headers, body = error.code, error.headers, error.read()
    assert status == expected, (url, status, body[:500])
    return headers, body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--ffmpeg', default=shutil.which('ffmpeg'))
    parser.add_argument('--ffprobe', default=shutil.which('ffprobe'))
    parser.add_argument('--output-dir', type=Path, default=Path('target/web-hls-verification'))
    parser.add_argument('--serve-browser', action='store_true', help='Keep the isolated fixture and runtime running for browser checks')
    parser.add_argument('--copy-video', action='store_true', help='Verify H.264 B-frame copying with audio conversion instead of forced video encoding')
    parser.add_argument('--adjacent-tools', action='store_true', help='Require runtime media tools beside its executable with no PATH or tool overrides')
    args = parser.parse_args()
    if not args.ffmpeg or not args.ffprobe:
        parser.error('Provide FFmpeg and FFprobe executable paths')
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    media = output / 'unsupported.mkv'
    subtitle = output / 'fixture.srt'
    subtitle.write_text('1\n00:00:01,000 --> 00:00:03,000\nFirst subtitle\n\n2\n00:00:17,000 --> 00:00:19,000\nSeek subtitle\n', encoding='utf-8')
    command = [args.ffmpeg, '-hide_banner', '-loglevel', 'error', '-f', 'lavfi', '-i',
        'testsrc2=size=160x90:rate=24', '-f', 'lavfi', '-i',
        'anullsrc=channel_layout=5.1:sample_rate=48000', '-f', 'lavfi', '-i',
        'sine=frequency=440:sample_rate=48000', '-i', str(subtitle), '-t', '20',
        '-map', '0:v', '-map', '1:a', '-map', '2:a', '-map', '3:s',
        '-c:a', 'pcm_s16le', '-c:s', 'ass',
        '-metadata:s:a:0', 'language=eng', '-metadata:s:a:1', 'language=fra',
        '-metadata:s:s:0', 'language=eng', '-y', str(media)]
    video_options = (['-c:v', 'libx264', '-preset', 'medium', '-pix_fmt', 'yuv420p',
        '-x264-params', 'keyint=96:min-keyint=96:scenecut=0:bframes=3:open-gop=0']
        if args.copy_video else ['-c:v', 'mpeg4', '-q:v', '4'])
    command[-1:-1] = video_options
    subprocess.run(command, check=True, timeout=120)
    requests = []
    config = {}
    web_dir = Path(__file__).resolve().parents[1] / 'tests' / 'web'
    bundle = Path(__file__).resolve().parents[1] / 'target' / 'web-hls-browser' / 'player.js'

    class Origin(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            path = urlsplit(self.path).path
            requests.append({'path': self.path, 'range': self.headers.get('Range')})
            if path == '/test-config.json':
                body, mime = json.dumps(config).encode(), 'application/json'
            elif path in ('/', '/index.html'):
                body, mime = (web_dir / 'index.html').read_bytes(), 'text/html'
            elif path == '/player.js' and bundle.exists():
                body, mime = bundle.read_bytes(), 'application/javascript'
            elif path == '/unsupported.mkv':
                size = media.stat().st_size
                start, end = 0, size - 1
                match = re.fullmatch(r'bytes=(\d+)-(\d*)', self.headers.get('Range', ''))
                if match:
                    start = int(match[1])
                    end = min(int(match[2]) if match[2] else end, end)
                if start >= size:
                    self.send_response(416)
                    self.send_header('Content-Range', f'bytes */{size}')
                    self.end_headers()
                    return
                self.send_response(206 if match else 200)
                self.send_header('Content-Type', 'video/x-matroska')
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Content-Length', str(end - start + 1))
                if match:
                    self.send_header('Content-Range', f'bytes {start}-{end}/{size}')
                self.end_headers()
                if self.command == 'HEAD':
                    return
                try:
                    with media.open('rb') as source:
                        source.seek(start)
                        remaining = end - start + 1
                        while remaining:
                            data = source.read(min(remaining, 65536))
                            self.wfile.write(data)
                            remaining -= len(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(body)

    origin = ThreadingHTTPServer(('127.0.0.1', 0), Origin)
    thread = threading.Thread(target=origin.serve_forever, daemon=True)
    thread.start()
    source_url = f'http://127.0.0.1:{origin.server_port}/unsupported.mkv?token=fixture%2Bencoded&expires=123'
    log = (output / 'runtime.log').open('w', encoding='utf-8')
    environment = dict(os.environ, APP_PATH=str(output / 'runtime-data'),
        SETTINGS_PATH=str(output / 'runtime-data'), STREMIO_SERVICE_CACHE=str(output / 'cache'),
        STREMIO_SERVICE_PORT='0',
        STREMIO_FFMPEG_PATH=str(Path(args.ffmpeg).resolve()), STREMIO_FFPROBE_PATH=str(Path(args.ffprobe).resolve()),
        RUST_LOG='warn')
    if args.adjacent_tools:
        for name in ('STREMIO_FFMPEG_PATH', 'STREMIO_FFPROBE_PATH', 'FONTCONFIG_FILE'):
            environment.pop(name, None)
        environment['PATH'] = ''
    process = subprocess.Popen([str(args.runtime.resolve())], env=environment, stdout=log, stderr=subprocess.STDOUT)
    runtime_url = None
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('Runtime exited during startup; see runtime.log')
            text = (output / 'runtime.log').read_text(encoding='utf-8', errors='replace')
            match = re.search(r'EngineFS server started at (http://\S+)', text)
            if match:
                runtime_url = match[1]
                break
            time.sleep(0.1)
        assert runtime_url, 'Runtime did not announce its ephemeral port'
        config.update(runtimeURL=runtime_url, mediaURL=source_url, forceTranscoding=not args.copy_video)
        _, data = request(runtime_url + '/hlsv2/probe?' + urlencode({'mediaURL': source_url}))
        probe = json.loads(data)
        assert probe['format']['name'].find('matroska') >= 0
        assert abs(probe['format']['duration'] - 20) < 0.2
        audio = [item for item in probe['streams'] if item['track'] == 'audio']
        assert len(audio) == 2 and audio[0]['channels'] == 6
        assert [item['id'] for item in audio] == [0, 1]
        request(runtime_url + '/hlsv2/probe?' + urlencode({'mediaURL': 'file:///untrusted'}), 400)
        options = [('mediaURL', source_url), ('videoCodecs', 'h264'), ('audioCodecs', 'aac'),
            ('maxAudioChannels', '2')]
        if not args.copy_video:
            options.append(('forceTranscoding', '1'))
        query = urlencode(options)
        master_url = runtime_url + '/hlsv2/fixture-api/master.m3u8?' + query
        _, master = request(master_url)
        master = master.decode()
        assert master.startswith('#EXTM3U')
        track_urls = re.findall(r'URI="([^"]+)"', master)
        track_urls += [line for line in master.splitlines() if line and not line.startswith('#')]
        assert any('audio0' in url for url in track_urls) and any('audio1' in url for url in track_urls)
        assert any('video0' in url for url in track_urls)
        for track in ('video0', 'audio0', 'audio1'):
            track_url = urljoin(master_url, next(url for url in track_urls if track in url))
            _, playlist = request(track_url)
            playlist = playlist.decode()
            assert '#EXT-X-ENDLIST' in playlist
            init_url = re.search(r'#EXT-X-MAP:URI="([^"]+)"', playlist)[1]
            _, init = request(urljoin(track_url, init_url))
            segment_urls = [line for line in playlist.splitlines() if line and not line.startswith('#')]
            assert len(segment_urls) >= 3
            # Fetch the end first, then the beginning and adjacent segment: seeking
            # must not rely on a previous sequential conversion run.
            segments = {}
            for index in (len(segment_urls) - 1, 0, 1):
                _, segment = request(urljoin(track_url, segment_urls[index]))
                assert segment and b'moof' in segment
                segments[index] = segment
            for index in (0, len(segment_urls) - 1):
                path = output / f'{track}-segment-{index}.mp4'
                path.write_bytes(init + segments[index])
                result = subprocess.run([args.ffprobe, '-v', 'error', '-show_streams', '-of', 'json', str(path)],
                    check=True, capture_output=True, timeout=30)
                streams = json.loads(result.stdout)['streams']
                assert streams[0]['codec_name'] == ('h264' if track == 'video0' else 'aac')
                if track == 'audio0':
                    assert streams[0]['channels'] == 2
                subprocess.run([args.ffmpeg, '-v', 'error', '-i', str(path), '-f', 'null', '-'],
                    check=True, timeout=30, capture_output=True)
        _, source = request(runtime_url + '/hlsv2/fixture-api/source/subtitle/0.ass?' + query)
        assert b'[Script Info]' in source and b'First subtitle' in source
        if not args.copy_video:
            # Exercise the runtime burn endpoint, including packaged font discovery,
            # rather than only checking the FFmpeg filter directly.
            video_url = urljoin(master_url, next(url for url in track_urls if 'video0' in url))
            _, video_playlist = request(video_url)
            video_segments = [line for line in video_playlist.decode().splitlines()
                if line and not line.startswith('#')]
            last_segment_url = video_segments[-1]
            plain_file = output / f'video0-segment-{len(video_segments) - 1}.mp4'
            request(runtime_url + '/hlsv2/fixture-api/burn?' + query + '&id=0')
            _, burned_init = request(urljoin(video_url, re.search(r'#EXT-X-MAP:URI="([^"]+)"', video_playlist.decode())[1]))
            _, burned_segment = request(urljoin(video_url, last_segment_url))
            burned_file = output / 'burned-video.mp4'
            burned_file.write_bytes(burned_init + burned_segment)
            def frame(path):
                return subprocess.run([args.ffmpeg, '-v', 'error', '-i', str(path),
                    '-ss', '1.5', '-frames:v', '1', '-pix_fmt', 'rgb24', '-f', 'rawvideo', 'pipe:1'],
                    check=True, capture_output=True, timeout=30).stdout
            plain_pixels, burned_pixels = frame(plain_file), frame(burned_file)
            assert plain_pixels and len(plain_pixels) == len(burned_pixels)
            assert plain_pixels != burned_pixels, 'Runtime subtitle burn did not change any pixels'
            request(runtime_url + '/hlsv2/fixture-api/burn?' + query)
        _, data = request(runtime_url + '/hlsv2/status')
        status = json.loads(data)
        assert 'fixture-api' in status
        if args.copy_video:
            assert status['fixture-api']['status']['converters']['video0']['copy'] is True, status
        request(runtime_url + '/hlsv2/fixture-api/destroy')
        _, data = request(runtime_url + '/hlsv2/status')
        assert 'fixture-api' not in json.loads(data)
        results = {'passed': True, 'copyVideo': args.copy_video, 'adjacentTools': args.adjacent_tools,
            'runtimeURL': runtime_url, 'sourceURL': source_url,
            'browserFixture': f'http://127.0.0.1:{origin.server_port}/', 'requests': requests}
        (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({key: value for key, value in results.items() if key != 'requests'}
            | {'resultsFile': str(output / 'results.json')}), flush=True)
        if args.serve_browser:
            while process.poll() is None:
                time.sleep(0.2)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        origin.shutdown()
        origin.server_close()
        log.close()


if __name__ == '__main__':
    main()
