#!/usr/bin/env python3
"""Validate the real HLS probe against generated, local MP4/Matroska media.

Requires Python 3.9+, FFmpeg, FFprobe, and a built runtime. Runs an isolated
runtime on a random port; no installed application or provider URLs are used.
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
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import urlopen


QUERY = "token=local%2Bencoded&expires=123"


def request_json(url):
    try:
        response = urlopen(url, timeout=70)
    except HTTPError as response:
        return response.code, json.loads(response.read())
    with response:
        return response.status, json.loads(response.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--ffmpeg", default=shutil.which("ffmpeg"))
    parser.add_argument("--ffprobe", default=shutil.which("ffprobe"))
    parser.add_argument("--postman", help="Optional Postman CLI executable for request assertions")
    parser.add_argument("--output-dir", type=Path, default=Path("target/hls-probe-validation"))
    args = parser.parse_args()
    if not args.runtime.is_file() or not args.ffmpeg or not args.ffprobe:
        parser.error("provide an existing runtime, FFmpeg, and FFprobe")
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    mp4 = output / "video.mp4"
    subprocess.run([
        args.ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
        "testsrc2=size=160x90:rate=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
        "-t", "3", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "10", "-bf", "2",
        "-c:a", "aac", "-ac", "2", "-metadata:s:a:0", "language=eng", "-metadata:s:a:0",
        "title=English", "-movflags", "+faststart", "-y", str(mp4),
    ], check=True)
    tail = output / "tail.mp4"
    subprocess.run([args.ffmpeg, "-hide_banner", "-loglevel", "error", "-i", str(mp4), "-c", "copy", "-y", str(tail)], check=True)
    subtitle = output / "subtitle.srt"
    subtitle.write_text("1\n00:00:00,200 --> 00:00:02,000\nLocal subtitle fixture\n", encoding="utf-8")
    attachment = output / "attachment.txt"
    attachment.write_text("Local attachment fixture\n", encoding="utf-8")
    mkv = output / "video.mkv"
    subprocess.run([
        args.ffmpeg, "-hide_banner", "-loglevel", "error", "-i", str(mp4), "-i", str(subtitle),
        "-map", "0:v", "-map", "0:a", "-map", "1:0", "-c:v", "copy", "-c:a", "copy", "-c:s", "ass",
        "-metadata:s:s:0", "language=spa", "-metadata:s:s:0", "title=Spanish", "-attach", str(attachment),
        "-metadata:s:t:0", "mimetype=text/plain", "-y", str(mkv),
    ], check=True)
    media = {"/video.mp4": mp4.read_bytes(), "/tail.mp4": tail.read_bytes(), "/video.mkv": mkv.read_bytes(), "/invalid": b"not a media file"}
    requests = []

    class Origin(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            requests.append({"path": self.path, "range": self.headers.get("Range")})
            payload = media.get(urlsplit(self.path).path)
            if payload is None:
                self.send_error(404)
                return
            selected = re.fullmatch(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
            start = int(selected[1]) if selected else 0
            end = min(int(selected[2]) if selected and selected[2] else len(payload) - 1, len(payload) - 1)
            if start >= len(payload):
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(payload)}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(206 if selected else 200)
            if selected:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - start + 1))
            self.end_headers()
            try:
                self.wfile.write(payload[start:end + 1])
            except (BrokenPipeError, ConnectionResetError):
                pass

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    worker = threading.Thread(target=origin.serve_forever, daemon=True)
    worker.start()
    origin_url = f"http://127.0.0.1:{origin.server_port}"
    environment = os.environ.copy()
    app_path = output / "runtime-state"
    app_path.mkdir(exist_ok=True)
    environment.update({"APP_PATH": str(app_path), "SETTINGS_PATH": str(app_path),
                        "STREMIO_SERVICE_CACHE": str(app_path), "STREMIO_SERVICE_PORT": "0",
                        "STREMIO_FFPROBE_PATH": str(args.ffprobe), "STREMIO_FFMPEG_PATH": str(args.ffmpeg)})
    log_path = output / "runtime.log"
    log = log_path.open("wb")
    runtime = subprocess.Popen([str(args.runtime.resolve())], env=environment, stdout=log, stderr=subprocess.STDOUT)
    results = {}
    try:
        deadline = time.monotonic() + 20
        base = None
        while time.monotonic() < deadline:
            text = log_path.read_text(encoding="utf-8", errors="replace")
            match = re.search(r"EngineFS server started at (http://\S+)", text)
            if match:
                base = match[1]
                break
            if runtime.poll() is not None:
                raise RuntimeError("isolated runtime stopped before startup; inspect runtime.log")
            time.sleep(0.05)
        assert base, "isolated runtime did not start"
        for name in ["video.mp4", "tail.mp4", "video.mkv"]:
            media_url = f"{origin_url}/{name}?{QUERY}"
            status, response = request_json(base + "/hlsv2/probe?" + urlencode({"mediaURL": media_url, "samples": "1"}))
            assert status == 200, (name, status, response)
            assert response["format"]["duration"] >= 2.9
            video = next(stream for stream in response["streams"] if stream["track"] == "video")
            audio = next(stream for stream in response["streams"] if stream["track"] == "audio")
            assert (video["id"], video["index"], video["width"], video["height"], video["frameRate"]) == (0, 0, 160, 90, 10)
            assert (audio["id"], audio["index"], audio["channels"], audio["sampleRate"], audio["language"]) == (0, 1, 2, 48000, "eng")
            assert response["samples"][str(video["index"])]["samples"]
            packets = json.loads(subprocess.check_output([
                args.ffprobe, "-v", "error", "-select_streams", "v:0", "-show_packets", "-of", "json", media_url
            ]))["packets"]
            timing = response["samples"][str(video["index"])]["samples"]
            if name.endswith(".mp4"):
                expected = [("K" in packet["flags"], packet["pts"], packet["dts"], packet["duration"]) for packet in packets]
                actual = [(sample["key"], sample["pts"], sample["dts"], sample["duration"]) for sample in timing]
                assert actual == expected, (name, "sample timing differs from FFprobe packets", actual[:3], expected[:3])
            else:
                assert [sample["pts"] for sample in timing] == [packet["pts"] for packet in packets if "K" in packet["flags"]]
                subtitles = next(stream for stream in response["streams"] if stream["track"] == "subtitle")
                attached = next(stream for stream in response["streams"] if stream["track"] == "attachment")
                assert (subtitles["id"], subtitles["index"], subtitles["language"], subtitles["title"]) == (0, 2, "spa", "Spanish")
                assert attached["index"] == 3 and attached["mimeType"] == "text/plain" and attached["size"] > 0
            results[name] = response
        relative = "/proxy/" + quote(urlencode({"d": origin_url}), safe="") + "/video.mp4?" + QUERY
        status, response = request_json(base + "/hlsv2/probe?" + urlencode({"mediaURL": relative}))
        assert status == 200 and response["samples"] == {}, (status, response)
        for query in ["", urlencode({"mediaURL": "file:///tmp/video.mp4"}), "mediaURL=a&mediaURL=b"]:
            status, response = request_json(base + "/hlsv2/probe?" + query)
            assert status == 400 and response["error"]["code"] == 100
        status, response = request_json(base + "/hlsv2/probe?" + urlencode({"mediaURL": origin_url + "/invalid"}))
        assert status == 500 and response["error"]["code"] == 100
        if args.postman:
            assertions = output / "postman-probe-assertions.js"
            assertions.write_text("""pm.test('Browser probe contract', function () {
  pm.response.to.have.status(200);
  const body = pm.response.json();
  pm.expect(body.format.name).to.include('matroska');
  pm.expect(body.format.duration).to.be.above(2.9);
  pm.expect(body.samples).to.deep.equal({});
  const video = body.streams.find(stream => stream.track === 'video');
  const audio = body.streams.find(stream => stream.track === 'audio');
  pm.expect([video.id, video.index, video.codec, video.width, video.height, video.frameRate]).to.deep.equal([0, 0, 'h264', 160, 90, 10]);
  pm.expect([audio.id, audio.index, audio.channels, audio.sampleRate, audio.language]).to.deep.equal([0, 1, 2, 48000, 'eng']);
});\n""", encoding="utf-8")
            command = [args.postman, "request", base + "/hlsv2/probe?" + urlencode({"mediaURL": origin_url + "/video.mkv?" + QUERY}),
                       "--timeout", "65000", "--script-post-request", "@" + str(assertions), "--response-only"]
            checked = subprocess.run(command, check=True, capture_output=True, text=True)
            (output / "postman-probe-output.txt").write_text(checked.stdout + checked.stderr, encoding="utf-8")
        assert all(urlsplit(item["path"]).query == QUERY for item in requests if urlsplit(item["path"]).path != "/invalid")
        result_path = output / "hls-probe-results.json"
        result_path.write_text(json.dumps({"results": results, "requests": requests}, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"validated": True, "containers": list(results), "packet_timings_match": True,
                          "signed_query_preserved": True, "postman_assertions": 1 if args.postman else 0,
                          "results_file": str(result_path)}))
    finally:
        runtime.terminate()
        runtime.wait(timeout=10)
        log.close()
        origin.shutdown()
        origin.server_close()
        worker.join(timeout=2)


if __name__ == "__main__":
    main()
