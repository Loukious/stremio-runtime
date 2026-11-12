#!/usr/bin/env python3
"""Validate a native standalone runtime bundle and create its release archive."""

import argparse
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import queue
import re
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
import urllib.parse
import urllib.request
import zipfile


REPOSITORY = Path(__file__).resolve().parent.parent


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def run(command, timeout=120, cwd=None, env=None, stdout_only=False):
    arguments = [arg.as_posix() if isinstance(arg, Path) else str(arg) for arg in command]
    result = subprocess.run(arguments, capture_output=True, text=True, errors="replace", timeout=timeout, cwd=cwd, env=env)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {command}\n{result.stdout}\n{result.stderr}")
    return result.stdout if stdout_only else result.stdout + result.stderr


def architecture(path):
    with path.open("rb") as source:
        header = source.read(64)
        if header[:2] == b"MZ":
            source.seek(struct.unpack_from("<I", header, 60)[0])
            signature, machine = struct.unpack("<4sH", source.read(6))
            if signature != b"PE\0\0":
                raise RuntimeError(f"Invalid PE executable: {path}")
            return {0x8664: "x64", 0xAA64: "arm64"}.get(machine, hex(machine))
        if header[:4] == b"\x7fELF":
            if header[4:6] != b"\x02\x01":
                raise RuntimeError(f"Expected a little-endian 64-bit ELF executable: {path}")
            return {62: "x64", 183: "arm64"}.get(struct.unpack_from("<H", header, 18)[0], "unknown")
        if header[:4] == b"\xcf\xfa\xed\xfe":
            return {0x1000007: "x64", 0x100000C: "arm64"}.get(struct.unpack_from("<I", header, 4)[0], "unknown")
        raise RuntimeError(f"Unsupported executable format: {path}")


def dependencies(executable, operating_system):
    if operating_system == "windows":
        listing = run(["dumpbin", "/nologo", "/dependents", executable])
        names = sorted(set(re.findall(r"^\s+([\w.-]+\.dll)\s*$", listing, re.MULTILINE | re.IGNORECASE)))
        if not names:
            raise RuntimeError(f"Could not inspect PE imports: {executable}")
        system32 = Path(os.environ["SystemRoot"]) / "System32"
        for name in names:
            if not name.lower().startswith(("api-ms-win-", "ext-ms-win-")) and not (system32 / name).exists():
                raise RuntimeError(f"Unbundled Windows dependency: {name}")
            if executable.name == "stremio-runtime.exe" and re.match(r"(?:vcruntime|msvcp|ucrtbase)", name, re.IGNORECASE):
                raise RuntimeError(f"Runtime was not built with static MSVC CRT: {name}")
        return {"imports": names}
    if operating_system == "macos":
        listing = run(["otool", "-L", executable])
        names = [line.strip().split(" (", 1)[0] for line in listing.splitlines()[1:] if line.strip()]
        for name in names:
            if not name.startswith(("/usr/lib/", "/System/Library/")):
                raise RuntimeError(f"Unbundled macOS dependency: {name}")
        load_commands = run(["otool", "-l", executable])
        versions = re.findall(r"^\s+(?:minos|version)\s+(\d+\.\d+(?:\.\d+)?)\s*$", load_commands, re.MULTILINE)
        if any(tuple(map(int, version.split("."))) > (13, 0, 0) for version in versions):
            raise RuntimeError(f"Executable requires macOS newer than 13: {versions}")
        return {"imports": names, "minimum_macos": versions}
    listing = run(["readelf", "-d", executable])
    names = re.findall(r"Shared library: \[([^\]]+)\]", listing)
    system_libraries = {"libc.so.6", "libm.so.6", "libdl.so.2", "libpthread.so.0", "librt.so.1", "libgcc_s.so.1", "ld-linux-x86-64.so.2", "ld-linux-aarch64.so.1"}
    for name in names:
        if name not in system_libraries:
            raise RuntimeError(f"Unbundled Linux dependency: {name}; link it statically or bundle it explicitly")
    versions = run(["readelf", "--version-info", executable])
    glibc = sorted(set(re.findall(r"\bGLIBC_(\d+\.\d+(?:\.\d+)?)\b", versions)), key=lambda value: tuple(map(int, value.split("."))))
    if glibc and tuple(map(int, glibc[-1].split("."))) > (2, 35, 0):
        raise RuntimeError(f"Executable requires glibc newer than 2.35: {glibc[-1]}")
    return {"imports": names, "minimum_glibc": glibc[-1] if glibc else None}


def runtime_licenses(bundle, cargo):
    metadata = json.loads(run([cargo, "metadata", "--locked", "--format-version", "1"], cwd=REPOSITORY, stdout_only=True))
    licenses = bundle / "licenses" / "rust"
    inventory = []
    for package in sorted(metadata["packages"], key=lambda item: (item["name"], item["version"])):
        if package["id"] in metadata["workspace_members"]:
            continue
        directory = Path(package["manifest_path"]).parent
        files = set()
        # Workspace crates (including librqbit) often keep licenses at a parent root.
        for candidate in [directory, *list(directory.parents)[:3]]:
            for path in candidate.iterdir():
                if path.is_file() and path.name.upper().startswith(("LICENSE", "COPYING", "COPYRIGHT", "NOTICE")):
                    files.add(path)
        if package.get("license_file"):
            files.add(directory / package["license_file"])
        copied = []
        for index, path in enumerate(sorted(files)):
            if path.is_file():
                destination = licenses / f"{package['name']}-{package['version']}" / f"{index}-{path.name}"
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
                copied.append(destination.relative_to(bundle).as_posix())
        inventory.append({"name": package["name"], "version": package["version"], "license": package.get("license"), "repository": package.get("repository"), "source": package.get("source"), "license_files": copied})
    (bundle / "RUST-THIRD-PARTY-NOTICES.json").write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")


class QuietHTTPHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


def request_json(url):
    with urllib.request.urlopen(url, timeout=45) as response:
        return json.load(response)


def runtime_smoke(runtime, media, temporary):
    fixture = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHTTPHandler, directory=str(media.parent)))
    threading.Thread(target=fixture.serve_forever, daemon=True).start()
    media_url = f"http://127.0.0.1:{fixture.server_port}/{media.name}"
    env = os.environ.copy()
    for name in ("STREMIO_FFMPEG_PATH", "STREMIO_FFPROBE_PATH"):
        env.pop(name, None)
    env.update({"APP_PATH": str(temporary / "app"), "SETTINGS_PATH": str(temporary / "settings"), "STREMIO_SERVICE_CACHE": str(temporary / "cache"), "STREMIO_SERVICE_PORT": "0"})
    process = subprocess.Popen([str(runtime)], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
    output = queue.Queue()
    threading.Thread(target=lambda: [output.put(line) for line in process.stdout], daemon=True).start()
    diagnostics = []
    try:
        deadline = time.monotonic() + 60
        base = None
        while time.monotonic() < deadline:
            try:
                line = output.get(timeout=0.5)
            except queue.Empty:
                if process.poll() is not None:
                    raise RuntimeError("Runtime exited during startup: " + "".join(diagnostics[-20:]))
                continue
            diagnostics.append(line)
            match = re.search(r"EngineFS server started at (http://127\.0\.0\.1:\d+)", line)
            if match:
                base = match[1]
                break
        if base is None:
            raise RuntimeError("Runtime did not announce its HTTP endpoint: " + "".join(diagnostics[-20:]))
        if request_json(base + "/heartbeat").get("success") is not True:
            raise RuntimeError("Runtime heartbeat did not succeed")
        probe = request_json(base + "/probe?" + urllib.parse.urlencode({"url": media_url}))
        if probe.get("error") or not probe.get("streams"):
            raise RuntimeError(f"Runtime could not find/probe adjacent FFprobe: {probe}")
        return {"heartbeat": True, "adjacent_ffprobe": True}
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        fixture.shutdown()
        fixture.server_close()


def verify_tools(bundle, platform, check_hls):
    operating_system, cpu = platform.split("-")
    suffix = ".exe" if operating_system == "windows" else ""
    paths = {name: (bundle / (name + suffix)).resolve() for name in ("stremio-runtime", "ffmpeg", "ffprobe")}
    report = {}
    for name, path in paths.items():
        actual = architecture(path)
        if actual != cpu:
            raise RuntimeError(f"Wrong architecture for {name}: expected {cpu}, got {actual}")
        report[name] = {"architecture": actual, **dependencies(path, operating_system)}
    ffmpeg, ffprobe = paths["ffmpeg"], paths["ffprobe"]
    report["ffmpeg"]["version"] = run([ffmpeg, "-version"])
    report["ffprobe"]["version"] = run([ffprobe, "-version"])
    encoders = run([ffmpeg, "-hide_banner", "-encoders"])
    decoders = run([ffmpeg, "-hide_banner", "-decoders"])
    protocols = run([ffmpeg, "-hide_banner", "-protocols"])
    for name in ("libx264", "aac"):
        if not re.search(rf"^\s*[A-Z.]{{6}}\s+{name}\s", encoders, re.MULTILINE):
            raise RuntimeError(f"Required encoder unavailable: {name}")
    for name in ("h264", "aac", "hevc", "vp9", "libdav1d", "ass", "webvtt"):
        if not re.search(rf"^\s*[A-Z.]{{6}}\s+{name}\s", decoders, re.MULTILINE):
            raise RuntimeError(f"Required decoder unavailable: {name}")
    for name in ("http", "https", "file", "tcp", "tls"):
        if name not in protocols.split():
            raise RuntimeError(f"Required protocol unavailable: {name}")
    filters = run([ffmpeg, "-hide_banner", "-filters"])
    if not re.search(r"^\s*[A-Z.]+\s+subtitles\s", filters, re.MULTILINE):
        raise RuntimeError("Required libass subtitles filter is unavailable")
    if "noise" not in run([ffmpeg, "-hide_banner", "-bsfs"]).split():
        raise RuntimeError("Required H.264 copy boundary bitstream filter is unavailable")
    with tempfile.TemporaryDirectory(prefix="stremio-release-smoke-") as directory:
        temporary = Path(directory)
        media = temporary / "sample.mp4"
        run([ffmpeg, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=24", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "9", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac", "-movflags", "+faststart", media])
        metadata = json.loads(run([ffprobe, "-v", "error", "-show_format", "-show_streams", "-of", "json", media], stdout_only=True))
        if {stream["codec_name"] for stream in metadata["streams"]} != {"h264", "aac"}:
            raise RuntimeError("Generated H.264/AAC sample did not probe correctly")
        playlist = temporary / "tools.m3u8"
        run([ffmpeg, "-v", "error", "-i", media, "-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", "-f", "hls", "-hls_time", "2", "-hls_playlist_type", "vod", "-hls_segment_type", "fmp4", playlist])
        run([ffmpeg, "-v", "error", "-i", playlist, "-f", "null", "-"])
        shutil.copytree(bundle / "fonts", temporary / "fonts")
        (temporary / "smoke.ass").write_text(
            "[Script Info]\nScriptType: v4.00+\nPlayResX: 320\nPlayResY: 180\n"
            "[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n"
            "Style: Default,DejaVu Sans,28,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,0,2,10,10,10,1\n"
            "[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
            "Dialogue: 0,0:00:00.00,0:00:09.00,Default,,0,0,0,,Standalone ASS burn test\n",
            encoding="utf-8",
        )
        burn_env = os.environ.copy()
        burn_env["FONTCONFIG_FILE"] = str((bundle / "fonts.conf").resolve())
        for filename, filter_arguments in (("plain.rgb", []), ("burn.rgb", ["-vf", "subtitles=smoke.ass:fontsdir=fonts"])):
            run([ffmpeg, "-v", "error", "-i", media, *filter_arguments, "-frames:v", "1", "-pix_fmt", "rgb24", "-f", "rawvideo", filename], cwd=temporary, env=burn_env)
        if (temporary / "plain.rgb").read_bytes() == (temporary / "burn.rgb").read_bytes():
            raise RuntimeError("ASS subtitles filter produced no visible text")
        report["smoke"] = {"h264_aac_roundtrip": True, "ffmpeg_fmp4_hls": True, "ass_burn": True, **runtime_smoke(paths["stremio-runtime"], media, temporary)}
        if check_hls:
            verification = temporary / "web-hls-verification"
            run([sys.executable, REPOSITORY / "scripts" / "verify-web-hls.py", "--runtime", paths["stremio-runtime"], "--ffmpeg", ffmpeg, "--ffprobe", ffprobe, "--output-dir", verification], timeout=300)
            report["smoke"]["runtime_hls"] = json.loads((verification / "results.json").read_text())
            copied_verification = temporary / "web-hls-copy-verification"
            run([sys.executable, REPOSITORY / "scripts" / "verify-web-hls.py", "--runtime", paths["stremio-runtime"], "--ffmpeg", ffmpeg, "--ffprobe", ffprobe, "--output-dir", copied_verification, "--copy-video"], timeout=300)
            report["smoke"]["runtime_hls_video_copy"] = json.loads((copied_verification / "results.json").read_text())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--smoke-hls", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+-]*", args.version):
        parser.error("version must contain only letters, numbers, '.', '+', and '-'")
    if args.platform not in {"windows-x64", "windows-arm64", "linux-x64", "linux-arm64", "macos-x64", "macos-arm64"}:
        parser.error("unsupported platform")
    bundle = args.bundle.resolve()
    bundle.mkdir(parents=True, exist_ok=True)
    runtime_name = "stremio-runtime.exe" if args.platform.startswith("windows-") else "stremio-runtime"
    shutil.copy2(args.runtime, bundle / runtime_name)
    shutil.copy2(REPOSITORY / "README.md", bundle / "RUNTIME-README.md")
    shutil.copy2(REPOSITORY / "docs" / "standalone-releases.md", bundle / "README.md")
    if not args.platform.startswith("windows-"):
        for name in ("stremio-runtime", "ffmpeg", "ffprobe"):
            (bundle / name).chmod(0o755)
    if args.platform.startswith("macos-"):
        for name in ("stremio-runtime", "ffmpeg", "ffprobe"):
            run(["codesign", "--force", "--sign", "-", bundle / name])
    runtime_licenses(bundle, args.cargo)
    report = verify_tools(bundle, args.platform, args.smoke_hls)
    report.update({"platform": args.platform, "package_version": args.version, "runtime_version": tomllib.loads((REPOSITORY / "Cargo.toml").read_text())["package"]["version"], "commit": run(["git", "rev-parse", "HEAD"], cwd=REPOSITORY).strip(), "working_tree_dirty": bool(run(["git", "status", "--porcelain"], cwd=REPOSITORY).strip())})
    (bundle / "BUILD-INFO.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    checksum_lines = []
    for path in sorted(bundle.rglob("*")):
        if path.is_file() and path.name != "SHA256SUMS.txt":
            checksum_lines.append(f"{digest(path)}  {path.relative_to(bundle).as_posix()}\n")
    (bundle / "SHA256SUMS.txt").write_text("".join(checksum_lines), encoding="utf-8")
    args.output.mkdir(parents=True, exist_ok=True)
    name = f"stremio-runtime-{args.version}-{args.platform}"
    if args.platform.startswith("windows-"):
        archive = args.output / (name + ".zip")
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as destination:
            for path in sorted(bundle.rglob("*")):
                if path.is_file():
                    destination.write(path, str(Path(name) / path.relative_to(bundle)))
    else:
        archive = args.output / (name + ".tar.gz")
        with tarfile.open(archive, "w:gz") as destination:
            destination.add(bundle, arcname=name)
    archive_digest = digest(archive)
    archive.with_name(archive.name + ".sha256").write_text(f"{archive_digest}  {archive.name}\n", encoding="utf-8")
    print(f"Validated {args.platform}: {archive} (SHA-256 {archive_digest})")


if __name__ == "__main__":
    main()
