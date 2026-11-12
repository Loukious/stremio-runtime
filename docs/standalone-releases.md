# Standalone runtime releases

Each desktop archive contains `stremio-runtime`, `ffmpeg`, and `ffprobe` in the
same directory (`.exe` on Windows). Extract the whole archive and launch the
runtime, or point the desktop shell at that executable. Keep all three programs
together so media probing and HLS conversion can find the adjacent tools.

The runtime prints `EngineFS server started at http://127.0.0.1:<port>` when ready.
Its preferred port is 11470, with an available random port selected on conflict.
Pass `--multi-user` for shared-machine torrent retention. `STREMIO_FFMPEG_PATH`
and `STREMIO_FFPROBE_PATH` can override the adjacent executables. Settings and
cache use the normal runtime locations; `APP_PATH`, `SETTINGS_PATH`, and
`STREMIO_SERVICE_CACHE` provide explicit overrides.

| Archive platform | CPU | Host requirements |
| --- | --- | --- |
| `windows-x64` | x86-64 | Windows 10 22H2 or newer |
| `windows-arm64` | AArch64 | Windows 11 ARM64 |
| `linux-x64` | x86-64 | glibc-based Linux, glibc 2.35 or newer, system CA certificates |
| `linux-arm64` | AArch64 | glibc-based Linux, glibc 2.35 or newer, system CA certificates |
| `macos-x64` | Intel x86-64 | macOS 13 or newer |
| `macos-arm64` | Apple Silicon | macOS 13 or newer |

Linux builds use Ubuntu 22.04 containers on native runners. OpenSSL and media
libraries are linked statically; glibc, the Linux loader, and ordinary system
libraries remain host dependencies. The archives support this documented glibc
baseline and are not musl/Alpine binaries. `BUILD-INFO.json` records each program's
architecture, actual imports, and highest required glibc symbol version.

Windows runtime builds use the static MSVC CRT; media tools use static MSYS2 Clang
libraries and Windows Schannel. macOS tools use system Secure Transport and are
signed ad hoc, without Apple notarization. Windows ARM64 x264 uses portable C
instead of its MSVC-specific assembly pipeline, so CPU transcoding can be slower
than an assembly-enabled build.

FFmpeg includes H.264 encoding through x264, AAC encoding, software AV1 decoding
through dav1d, and native HEVC/VP9/AAC decoders. HTTP(S), MPEG-TS, MP4/fMP4, HLS,
and subtitle demuxing remain enabled. ASS/WebVTT extraction and libass subtitle
burn-in are supported. Bundled DejaVu fonts provide a fallback when system fonts
are unavailable. Hardware acceleration
depends on OS/driver support and is not required for the software HLS fallback.

`SHA256SUMS.txt` covers every file in the archive. The accompanying `.sha256`
asset covers the archive itself. `BUILD-INFO.json` and `MEDIA-BUILD-INFO.txt`
record the runtime revision, versions, compiler/configuration, dependency checks,
and smoke-test results. `licenses/` and `RUST-THIRD-PARTY-NOTICES.json` preserve
third-party licensing information.

FFmpeg and FFprobe are separate GPL programs. `sources/` contains the complete,
checksum-pinned FFmpeg, x264, dav1d, libass, FreeType, HarfBuzz, and FriBidi source archives used to build them,
including the build helper and lock file. Linux bundles also include the OpenSSL
sources used for FFmpeg and the runtime's static TLS dependency, along with
Fontconfig and Expat for font discovery. Windows bundles include the matching
GNU libiconv sources used by libass for character conversion. This avoids
depending on a third-party binary provider to retain matching codec sources.

## Build or publish

The `Standalone runtime bundles` workflow builds all six platforms, runs the Rust
tests, checks executable architectures and dependency closure, exercises H.264/AAC
encoding/probing, visible ASS burn-in and fMP4 HLS decoding, and starts the packaged runtime to verify
its heartbeat, adjacent FFprobe discovery, and an HTTP-to-HLS conversion.

A manual run with `publish=false` uploads workflow artifacts only. A `v*` tag push
publishes the verified ZIP/tar.gz archives and archive checksums after all six jobs
succeed. Manual publication requires `publish=true` and an existing version tag.
The tag must match `Cargo.toml`, including any prerelease suffix. Publication uses
the workflow's GitHub token; extra secrets are unnecessary. Existing release
assets are not silently overwritten.

For a local source build, use a security-patched Python 3.10+ (with `tomli==2.3.0`
on Python 3.10), Rust 1.99.1, a native C toolchain,
Make, Meson 1.10.0+, Ninja, NASM, Perl, pkg-config, and CMake on Linux. Windows media tools need
MSYS2 CLANG64 or CLANGARM64; the Rust executable needs MSVC. macOS uses Xcode
command-line tools. Linux uses a glibc-based build environment.

```sh
python scripts/prepare-ffmpeg.py --platform linux-x64 --output bundle
bash scripts/build-media-tools.sh linux-x64 bundle
OPENSSL_DIR="$PWD/.release-cache/build/prefix" OPENSSL_STATIC=1 \
  cargo build --locked --release --target x86_64-unknown-linux-gnu
python scripts/package-runtime.py --platform linux-x64 \
  --runtime target/x86_64-unknown-linux-gnu/release/stremio-runtime \
  --bundle bundle --output dist --version 0.2.0 --smoke-hls
```

To rebuild just the media tools from a release's included sources without network
access, place their `.tar.gz` and font `.tar.bz2` files in `.release-cache/downloads/`, keep the
helpers and `fonts.conf` together in `sources/`, run `python sources/prepare-ffmpeg.py` with the
appropriate platform/output arguments, then run `bash sources/build-media-tools.sh`.
The helper verifies every archive against `sources/ffmpeg-lock.json` before reuse.
Build tools and OS libraries remain prerequisites; the result is pinned to source
revisions and flags, without claiming byte-for-byte reproducibility across
different compiler versions.
