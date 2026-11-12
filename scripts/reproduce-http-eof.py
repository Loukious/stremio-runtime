#!/usr/bin/env python3
"""Reproduce early HTTP EOF with isolated libmpv and a generated local MP4.

Requires Python 3.9+, FFmpeg, and a libmpv shared library. No user MPV config,
scripts, real media URLs, display, or audio device are used. The reload scenario
models the shell's duration-aware loadfile/FileLoaded/absolute-seek sequence;
it does not execute the Stremio shell itself.
"""

import argparse
import ctypes
import json
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


SIGNED_QUERY = "token=local-test%2Bencoded&expires=123"
RECONNECT_OPTIONS = (
    "reconnect=1,reconnect_on_network_error=1,"
    "reconnect_on_http_error=%23%408,429,500,502,503,504,reconnect_delay_max=15"
)


def recovery_eligible(snapshot):
    duration = snapshot.get("duration")
    position = snapshot.get("time_pos")
    return (
        snapshot.get("reason") == 0 and snapshot.get("seekable") == 1
        and duration is not None and position is not None
        and math.isfinite(duration) and math.isfinite(position)
        and duration > 30 and 0 < position < duration - 10
    )


class Event(ctypes.Structure):
    _fields_ = [("event_id", ctypes.c_int), ("error", ctypes.c_int),
                ("reply_userdata", ctypes.c_uint64), ("data", ctypes.c_void_p)]


class EndFile(ctypes.Structure):
    # These are the first two fields of mpv_event_end_file on all supported APIs.
    _fields_ = [("reason", ctypes.c_int), ("error", ctypes.c_int)]


class PropertyChange(ctypes.Structure):
    _fields_ = [("name", ctypes.c_char_p), ("format", ctypes.c_int), ("data", ctypes.c_void_p)]


class Mpv:
    def __init__(self, library, speed):
        self.dll_search = None
        if os.name == "nt":
            self.dll_search = os.add_dll_directory(str(library.parent))
        self.api = ctypes.CDLL(str(library))
        self.api.mpv_create.restype = ctypes.c_void_p
        self.api.mpv_initialize.argtypes = [ctypes.c_void_p]
        self.api.mpv_set_option_string.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        self.api.mpv_command.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)]
        self.api.mpv_get_property.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                            ctypes.c_int, ctypes.c_void_p]
        self.api.mpv_get_property_string.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        self.api.mpv_get_property_string.restype = ctypes.c_void_p
        self.api.mpv_free.argtypes = [ctypes.c_void_p]
        self.api.mpv_observe_property.argtypes = [ctypes.c_void_p, ctypes.c_uint64,
                                                ctypes.c_char_p, ctypes.c_int]
        self.api.mpv_wait_event.argtypes = [ctypes.c_void_p, ctypes.c_double]
        self.api.mpv_wait_event.restype = ctypes.POINTER(Event)
        self.api.mpv_terminate_destroy.argtypes = [ctypes.c_void_p]
        self.handle = self.api.mpv_create()
        if not self.handle:
            raise RuntimeError("mpv_create failed")
        options = {
            "config": "no", "load-scripts": "no", "ytdl": "no",
            "terminal": "no", "vo": "null", "ao": "null", "idle": "yes",
            "keep-open": "no", "speed": str(speed), "stream-lavf-o": RECONNECT_OPTIONS,
        }
        try:
            for name, value in options.items():
                result = self.api.mpv_set_option_string(self.handle, name.encode(), value.encode())
                if result < 0:
                    raise RuntimeError(f"MPV option {name} failed ({result})")
            if self.api.mpv_initialize(self.handle) < 0:
                raise RuntimeError("mpv_initialize failed")
            version = self.api.mpv_get_property_string(self.handle, b"mpv-version")
            self.version = ctypes.string_at(version).decode(errors="replace") if version else None
            if version:
                self.api.mpv_free(version)
            for key, (name, fmt) in enumerate([(b"time-pos", 5), (b"duration", 5), (b"seekable", 3)]):
                self.api.mpv_observe_property(self.handle, key, name, fmt)
        except BaseException:
            self.close()
            raise

    def property(self, name, flag=False):
        value = ctypes.c_int() if flag else ctypes.c_double()
        result = self.api.mpv_get_property(self.handle, name.encode(), 3 if flag else 5,
                                           ctypes.byref(value))
        return value.value if result >= 0 else None

    def command(self, *args):
        values = (ctypes.c_char_p * (len(args) + 1))(*(arg.encode() for arg in args), None)
        result = self.api.mpv_command(self.handle, values)
        if result < 0:
            raise RuntimeError(f"MPV command {args[0]} failed ({result})")

    def close(self):
        if self.handle:
            self.api.mpv_terminate_destroy(self.handle)
            self.handle = None
        if self.dll_search:
            self.dll_search.close()
            self.dll_search = None


def run_case(library, media, duration, speed, recover):
    requests = []
    prefix_length = max(16384, len(media) // 7)

    class Origin(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_GET(self):
            range_value = self.headers.get("Range")
            requests.append({"path": self.path, "range": range_value})
            offset = int(range_value.split("=")[1].split("-")[0]) if range_value else 0
            if offset >= len(media):
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(media)}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            # The first response is internally consistent HTTP 200, but contains
            # only a prefix of a container whose metadata declares the full duration.
            short = len(requests) == 1
            end = prefix_length if short else len(media)
            payload = media[offset:end]
            self.send_response(200 if short or not range_value else 206)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Accept-Ranges", "bytes")
            if range_value and not short:
                self.send_header("Content-Range", f"bytes {offset}-{end-1}/{len(media)}")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(payload)
                self.wfile.flush()
                self.connection.shutdown(socket.SHUT_WR)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    player = None
    started = time.monotonic()
    eof_events = []
    reloads = []
    pending_reload_seek = None
    latest = {"time_pos": None, "duration": None, "seekable": None}
    max_position = 0.0
    event_trace = []
    url = f"http://127.0.0.1:{server.server_port}/video.mp4?{SIGNED_QUERY}"
    try:
        player = Mpv(library, speed)

        def trace_event(event):
            if not event.event_id:
                return
            item = {"id": event.event_id, "wall_seconds": round(time.monotonic() - started, 3)}
            if event.event_id == 22:  # MPV_EVENT_PROPERTY_CHANGE
                change = ctypes.cast(event.data, ctypes.POINTER(PropertyChange)).contents
                item["property"] = change.name.decode(errors="replace")
                if change.data and change.format in (3, 5):
                    value_type = ctypes.c_int if change.format == 3 else ctypes.c_double
                    item["value"] = ctypes.cast(change.data, ctypes.POINTER(value_type)).contents.value
                else:
                    item["value"] = None
            event_trace.append(item)

        player.command("loadfile", url, "replace")
        deadline = started + max(15.0, duration / speed * 2 + 5)
        while time.monotonic() < deadline:
            event = player.api.mpv_wait_event(player.handle, 0.02).contents
            trace_event(event)
            if event.event_id == 8 and pending_reload_seek is not None:  # MPV_EVENT_FILE_LOADED
                player.command("seek", str(pending_reload_seek), "absolute")
                reloads[-1].update({
                    "seek_command": ["seek", str(pending_reload_seek), "absolute"],
                    "seek_after_event": event.event_id,
                    "seek_wall_seconds": round(time.monotonic() - started, 3),
                })
                pending_reload_seek = None
            for name, flag in [("time-pos", False), ("duration", False), ("seekable", True)]:
                value = player.property(name, flag)
                if value is not None:
                    latest[name.replace("-", "_")] = value
            max_position = max(max_position, latest["time_pos"] or 0.0)
            if event.event_id == 7:  # MPV_EVENT_END_FILE
                ended = ctypes.cast(event.data, ctypes.POINTER(EndFile)).contents
                snapshot = {
                    **latest, "reason": ended.reason, "error": ended.error,
                    "seekable_queried_at_end_event": player.property("seekable", True),
                    "wall_seconds": round(time.monotonic() - started, 3),
                }
                eof_events.append(snapshot)
                if recover and len(eof_events) == 1:
                    if not recovery_eligible(snapshot):
                        break
                    resume = latest["time_pos"] or 0.0
                    player.command("loadfile", url, "replace")
                    reloads.append({"url": url, "resume_position": resume})
                    pending_reload_seek = resume
                    latest = {"time_pos": None, "duration": None, "seekable": None}
                    continue
                break
        # Record queued teardown notifications without changing the EOF snapshot.
        for _ in range(10):
            event = player.api.mpv_wait_event(player.handle, 0.02).contents
            trace_event(event)
            if not event.event_id:
                break
        return {
            "case": "guarded_reload" if recover else "existing_reconnect_flags",
            "resource_bytes": len(media), "first_response_bytes": prefix_length,
            "generated_duration": duration, "playback_speed": speed,
            "mpv_version": player.version, "event_trace": event_trace[-40:],
            "eof_events": eof_events, "reloads": reloads, "max_time_pos": max_position,
            "requests": requests,
            "query_preserved": all(urlsplit(req["path"]).query == SIGNED_QUERY for req in requests),
        }
    finally:
        if player:
            player.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mpv-library", type=Path, required=True,
                        help="Path to libmpv-2.dll, libmpv.so, or libmpv.dylib")
    parser.add_argument("--ffmpeg", default=shutil.which("ffmpeg"),
                        help="FFmpeg executable (defaults to PATH)")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parents[1] / "target" / "http-eof-repro")
    parser.add_argument("--duration", type=float, default=40.0,
                        help="Fixture duration in seconds; must exceed the shell's 30-second gate")
    parser.add_argument("--speed", type=float, default=10.0,
                        help="Accelerated headless playback speed")
    args = parser.parse_args()
    library = args.mpv_library.expanduser().resolve()
    if not library.is_file():
        parser.error("--mpv-library must identify an existing shared library")
    if not args.ffmpeg:
        parser.error("FFmpeg is missing; provide --ffmpeg")
    if not math.isfinite(args.duration) or args.duration <= 30 or not 0 < args.speed <= 100:
        parser.error("duration must be finite and greater than 30 seconds; speed must be in (0, 100]")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fixture = args.output_dir / "http-eof-fixture.mp4"
    subprocess.run([
        args.ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
        "testsrc2=size=128x72:rate=10", "-f", "lavfi", "-i",
        "sine=frequency=440:sample_rate=16000", "-t", str(args.duration),
        "-c:v", "mpeg4", "-q:v", "5", "-c:a", "aac", "-b:a", "32k",
        "-movflags", "+faststart", "-y", str(fixture),
    ], check=True)
    media = fixture.read_bytes()
    results = [run_case(library, media, args.duration, args.speed, recover)
               for recover in (False, True)]
    for result in results:
        print(json.dumps(result), flush=True)
    output = args.output_dir / "http-eof-results.json"
    output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    before, after = results
    early = before["eof_events"][0] if before["eof_events"] else {}
    confirmed_early = (
        recovery_eligible(early) and early.get("error") == 0
        and early.get("duration", 0) >= args.duration - 0.2
    )
    recovered = (
        len(after["eof_events"]) == 2 and after["max_time_pos"] >= args.duration - 1
        and after["eof_events"][-1]["reason"] == 0 and after["eof_events"][-1]["error"] == 0
        and len(after["reloads"]) == 1 and after["reloads"][0].get("seek_after_event") == 8
    )
    preserved = all(result["query_preserved"] for result in results)
    print(json.dumps({"early_eof_reproduced": confirmed_early,
                      "guarded_reload_recovered": recovered,
                      "signed_query_preserved": preserved, "results_file": str(output)}))
    return 0 if confirmed_early and recovered and preserved else 2


if __name__ == "__main__":
    raise SystemExit(main())
