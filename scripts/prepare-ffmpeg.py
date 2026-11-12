#!/usr/bin/env python3
"""Fetch checksum-pinned media source trees and corresponding-source payloads."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import urllib.request


HERE = Path(__file__).resolve().parent
PLATFORMS = ["windows-x64", "windows-arm64", "linux-x64", "linux-arm64", "macos-x64", "macos-arm64"]


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def download(url, checksum, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and digest(destination) == checksum:
        return destination
    partial = destination.with_suffix(destination.suffix + ".part")
    with urllib.request.urlopen(url, timeout=120) as source, partial.open("wb") as output:
        shutil.copyfileobj(source, output)
    if digest(partial) != checksum:
        partial.unlink()
        raise RuntimeError(f"SHA-256 mismatch: {url}")
    partial.replace(destination)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, choices=PLATFORMS)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cache", default=Path(".release-cache"), type=Path)
    args = parser.parse_args()
    lock = json.loads((HERE / "ffmpeg-lock.json").read_text())
    payload = args.output / "sources"
    payload.mkdir(parents=True, exist_ok=True)
    for name, pin in lock["sources"].items():
        if "platforms" in pin and args.platform not in pin["platforms"]:
            continue
        archive = download(pin["url"], pin["sha256"], args.cache / "downloads" / f"{name}-{pin['revision']}.tar.gz")
        directory = args.cache / "source" / name
        marker = directory / ".source-pin.json"
        expected_pin = {"revision": pin["revision"], "sha256": pin["sha256"]}
        if directory.exists() and (not marker.is_file() or json.loads(marker.read_text()) != expected_pin):
            raise RuntimeError(f"Stale or unverified source cache at {directory}. Use a fresh --cache directory or remove this exact cache directory before rebuilding.")
        if not directory.exists():
            directory.mkdir(parents=True)
            with tarfile.open(archive) as source:
                source.extractall(directory, filter="data")
            marker.write_text(json.dumps(expected_pin), encoding="utf-8")
        shutil.copy2(archive, payload / archive.name)
        roots = [path for path in directory.iterdir() if path.is_dir()]
        if len(roots) != 1:
            raise RuntimeError(f"Expected exactly one {name} source directory")
        # HarfBuzz generators pass hundreds of source paths in one command. A
        # 40-character commit in every directory name can exceed Windows limits.
        if roots[0].name != "tree":
            roots[0].rename(directory / "tree")
            roots = [directory / "tree"]
        licenses = args.output / "licenses" / name
        licenses.mkdir(parents=True, exist_ok=True)
        candidates = [path for path in roots[0].iterdir() if path.is_file() and path.name.upper().startswith(("COPYING", "LICENSE", "COPYRIGHT", "NOTICE"))]
        if name == "freetype":
            candidates.extend(roots[0] / "docs" / filename for filename in ("FTL.TXT", "GPLv2.TXT"))
        for path in candidates:
            if path.is_file():
                shutil.copy2(path, licenses / path.name)
    font_pin = lock["fonts"]
    font_archive = download(font_pin["url"], font_pin["sha256"], args.cache / "downloads" / font_pin["file"])
    font_source = args.cache / "fonts"
    font_marker = font_source / ".source-pin.json"
    if font_source.exists() and (not font_marker.is_file() or font_marker.read_text() != font_pin["sha256"]):
        raise RuntimeError(f"Stale font cache at {font_source}. Use a fresh --cache directory before rebuilding.")
    if not font_source.exists():
        font_source.mkdir(parents=True)
        with tarfile.open(font_archive) as source:
            source.extractall(font_source, filter="data")
        font_marker.write_text(font_pin["sha256"], encoding="utf-8")
    font_root = next(path for path in font_source.iterdir() if path.is_dir())
    font_output = args.output / "fonts"
    font_output.mkdir(parents=True, exist_ok=True)
    for filename in ("DejaVuSans.ttf", "DejaVuSans-Bold.ttf", "DejaVuSans-Oblique.ttf", "DejaVuSans-BoldOblique.ttf"):
        font = font_root / "ttf" / filename
        shutil.copy2(font, font_output / font.name)
    font_licenses = args.output / "licenses" / "dejavu"
    font_licenses.mkdir(parents=True, exist_ok=True)
    shutil.copy2(font_root / "LICENSE", font_licenses / "LICENSE")
    shutil.copy2(font_archive, payload / font_archive.name)
    shutil.copy2(HERE / "fonts.conf", args.output / "fonts.conf")
    for filename in ("ffmpeg-lock.json", "prepare-ffmpeg.py", "build-media-tools.sh", "fonts.conf"):
        shutil.copy2(HERE / filename, payload / filename)
    shutil.copy2(HERE / "ffmpeg-lock.json", args.output / "ffmpeg-lock.json")
    (args.output / "FFMPEG-NOTICE.txt").write_text(
        "FFmpeg and FFprobe are separate GPL programs compiled from the exact complete\n"
        "source archives included in sources/. x264 supplies H.264 encoding; dav1d\n"
        "supplies software AV1 decoding. Linux also includes the matching OpenSSL\n"
        "sources used by these tools and by the runtime's static TLS dependency.\n"
        "libass, FreeType, HarfBuzz and FriBidi supply ASS subtitle rendering; Linux\n"
        "Fontconfig and Expat provide font discovery. Their exact sources are included.\n"
        "Windows libass character conversion uses the included GNU libiconv sources.\n"
        "Bundled DejaVu fonts use the separate license in licenses/dejavu.\n"
        "Windows HTTPS uses OS Schannel; macOS HTTPS uses OS Secure Transport.\n"
        "License texts are in licenses/. sources/build-media-tools.sh contains the\n"
        "build instructions; MEDIA-BUILD-INFO.txt records toolchain/configuration.\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
