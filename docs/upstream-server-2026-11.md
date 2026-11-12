# Upstream streaming server evaluation

Reviewed on 2026-11-12 against Stremio/stremio-shell-ng `d640dc18c006649059d3c338b83e4f6376f395b5`
(the final bundled server is 4.22.5) and this runtime's `8d7ad9c` baseline.
The comparison starts at the shell repositories' common ancestor
`264c3e6f5bb2ae9e76254e016563272ebaa1f006`, whose bundled server is 4.21.0.

This covers cumulative server updates `d7d4925` (4.21.1), `e383dd6` (4.21.2),
`81dde78` (4.22.1), `a35a4e8` (4.22.3), and `8d6244d` (4.22.5).
It compares the committed JavaScript module bodies with this runtime's source;
it does not run the original Node server or claim complete server compatibility.
Webpack module-number changes were removed before comparing bodies. Of 1,315 old
and 1,325 new module bodies, 43 old and 49 new bodies differ after that normalization.
Numeric module lookup maps still require manual classification as bundle metadata.

## Implemented runtime ports

| Upstream behavior | Rust adaptation |
| --- | --- |
| Serialized, temporary-file settings writes; failed saves do not become active | Hold the settings update lock through validation and atomic file replacement. Keep the previous active values on error and return JSON `error` with HTTP 500 for persistence failures. |
| Validate a selected cache root before saving it | Require an existing absolute directory, create a regular `stremio-cache` child, and check write access. Invalid selections return JSON `error` with HTTP 400. |
| Apply cache locations to future torrent engines while old engines retain their location | New magnet and `.torrent` additions read the saved root and reserve a folder per info hash. Existing handles continue in their original folders. Stats report that actual folder. This requires no runtime restart. |
| Retain previous cache locations for cleanup | Persist `cacheRootsToClean`, include old and current roots in the existing cache-size reaper, and protect active and initializing torrents. The reaper retains its existing size-limit policy; it does not erase an old root as soon as the user selects a new one. |
| Safe attachment filenames | Escape the ASCII fallback and supply a percent-encoded RFC 5987 UTF-8 filename, so Unicode, quotes, and control characters cannot break download headers. |
| Harden responses from the HTTP proxy | Set `X-Content-Type-Options: nosniff`, apply a sandbox CSP to non-WebUI sources, force untrusted HTML to download, and prevent HTML caching. Trust only the exact HTTPS Stremio WebUI hosts on the default HTTPS port, using the final response URL after redirects. Apply these protections after caller header overrides. |

`POST /settings` still returns `{"success":true}` after a successful save.
Validation and persistence failures return `{"error":"..."}` and leave the active
settings unchanged. An explicit `STREMIO_SERVICE_CACHE` environment override
continues to take precedence over the saved `cacheRoot`.
Fastresume bitfields stay in the startup session folder until the next runtime
start; torrent payloads use the new location immediately.

## Existing behavior and excluded changes

| Upstream behavior | Evaluation |
| --- | --- |
| Recover from exhausted fixed HTTP ports and propagate the chosen port | Already covered by `5e6bc8b` and the runtime's shared `base_url`. The shell consumes the advertised loopback URL. |
| Avoid a persisted DHT UDP port failing the next startup | Preserve `8d7ad9c`: the routing table remains persistent but the UDP port is allocated dynamically. |
| Fetch subtitle responses without Needle's automatic body parsing | The Rust subtitle path already reads response bytes/text directly through Reqwest. It does not use Needle or parse upstream subtitle bodies as JSON. |
| IPv4-only tracker filtering and tracker DNS changes | The runtime uses the custom librqbit engine with typed peer addresses and dual-stack sockets. Retain that implementation and its existing tracker list instead of copying Node torrent-stream limitations. |
| Embedded ASS extraction and parsing in the streaming server | The new `/embedded-ass` backend is outside the native external-ASS shell port. MPV already handles embedded subtitle tracks. This runtime does not add a duplicate Matroska extraction service. |
| Move the default Windows cache from the application-data path to `%LOCALAPPDATA%\Stremio` | Retain the custom runtime's current default and the user's saved location. The picker can select a new location explicitly; this port does not migrate or delete existing cache directories automatically. |
| Replace WMIC disk-space/cache-drive discovery with native or PowerShell calls | The Rust runtime does not invoke WMIC. The shell's folder chooser accepts the directory directly; no WMIC-dependent path is added. |
| FFmpeg/HLS probing, transcoding profiles, casting/VLC backend, NZB/FTP/archive changes, JS stream/dependency changes, and UWP packaging | These either belong to unimplemented optional backend routes or to Node-only dependencies. Preserve the custom runtime's no-transcoding architecture and existing downloader implementation. No `server.js` or FFmpeg bundle is restored. |

## Validation

`cargo +stable test --locked --bin stremio-runtime`: 19 tests passed on Windows x64.
The added checks cover concurrent settings writes, Windows read-only save failure,
cache-root validation, proxy HTML protections, and Unicode download filenames.
An offline librqbit test creates two complete one-byte torrents with DHT,
trackers, and local discovery disabled. It changes the cache folder between the
two additions, checks both physical folders, and verifies that cache pruning
preserves both active torrents.

A Windows x64 release build passed. Its runtime executable has SHA-256
`0851E50246F656601A0995D374EB017197029556083ED8613911A2E7BC78E295`.
The ARM64 release build also passed. Its runtime executable has SHA-256
`072C7428C9EDBFAC4F65FB9E8425262C484488C0BE578279A808C3E1663169FD`.
The installed Visual Studio toolchains lacked ARM64 CRT libraries. Instead of
modifying the system installation, the required Microsoft ARM64 CRT desktop and
shared import-library payloads were downloaded from the installed VS catalog,
checked against their catalog SHA-256 hashes, and extracted into the task-owned
`D:\Apps\stremio-arm64-builddeps` directory. The existing x64 linker was used
with those ARM64 libraries and the installed Windows SDK's ARM64 libraries.
The payload hashes are `c30a1064ef39efd3e969a1cac2bb56173de42aee558efbf7b5c1ca8910228687`
and `904230c4a5c5d34b84c9c962f9435a8909cdaa2d0ebe3d0c55472de4e45db081`.
ARM64 runtime execution was not tested on this x64 machine.
These local checks do not establish device/network playback behavior or the
unimplemented optional upstream server features.
