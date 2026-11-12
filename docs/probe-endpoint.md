# Media probing

`GET /probe?url=...` returns the legacy media probe object through an installed
FFprobe executable. `STREMIO_FFPROBE_PATH` selects an executable explicitly;
otherwise the runtime searches beside itself and then on `PATH`. No FFmpeg
binary is downloaded or bundled, and this does not add transcoding.

The endpoint accepts HTTP(S) URLs and relative streaming paths. Relative paths
use the runtime's advertised base URL, including its dynamically selected HTTP
port. It rejects local paths, other protocols, credentials, whitespace, and NUL bytes.
FFprobe receives separate command arguments and a protocol whitelist that excludes
local-file access. On Windows it runs without opening a console window.

Two probes may run concurrently. Additional requests return HTTP 503 immediately,
and active probes have a 15-second deadline. Stdout is limited to 1 MiB and stderr
to 64 KiB. Failure or timeout kills and reaps the child; cancellation also kills
the child and schedules reaping. HTTP 400 reports invalid input, HTTP 500 reports
executable or metadata failures, and HTTP 504 reports a timeout. Error bodies use
`{"error":"message","result":null}`.

The local JSON mapper reads only the fields needed by the legacy response. It
accepts missing optional fields and `N/A` metadata, retains video/audio/subtitle
indices, emits boolean default-track flags, and preserves fractional durations
and frame rates. Matroska maps to `mkv`; the MP4 family maps to `mp4`.
`/hlsv2/probe` retains its separate null-result stub.

The implementation adapts the contribution in
[PR #1](https://github.com/Loukious/stremio-runtime/pull/1). Its external typed
parser was replaced because valid FFprobe output can omit fields it requires;
the direct asynchronous invocation also permits bounded execution and cleanup.

Validation: all 27 locked runtime tests passed, including sparse metadata,
relative URLs, protocol rejection, real subprocess failure/output limits,
timeout, cancellation, and child termination checks. Windows x64 and ARM64
release builds use static CRT linking and import only Windows system DLLs.
Live HTTP checks with installed FFprobe cover valid sparse PCM WAV metadata,
fractional duration, relative paths through the proxy on a random HTTP port,
input and process errors, the HLSv2 stub, concurrent-probe rejection, and heartbeat
responsiveness. A local Postman request assertion also passed. ARM64 execution
still requires corresponding hardware; the FFprobe executable must support the
machine where the service runs.
