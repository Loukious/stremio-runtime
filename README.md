# stremio-service-rs

A Rust reimplementation of the Stremio Service, reverse-engineered from the
original server.js and the mobile app.

## What this is

The Stremio desktop app bundles a local HTTP server (`server.js` + `stremio-runtime`) that handles
torrent streaming, subtitle proxying, HLS transcoding, and other media duties. The app talks to it
over `localhost:11470` and expects a specific JSON/streaming HTTP contract.

This project reverse-engineers that contract and reimplements the server in Rust, with the goal of
being a drop-in replacement: the desktop shell launches this binary instead of `stremio-runtime.exe
server.js` and never knows the difference.

The torrent engine is powered by [librqbit](https://github.com/ikatson/rqbit) rather than the
original Node.js torrent-stream.

## Why

- The original Node.js server has known stability and performance issues, particularly around uTP
  peer connectivity.
- A native binary starts faster, uses less memory, and doesn't require bundling a JS runtime.
- Rust's async I/O is a natural fit for the byte-range streaming and concurrent torrent sessions
  the server needs to handle.

## Status and caveats

This is a **work in progress** based on black-box reverse engineering. The HTTP contract was
inferred by reading `server.js`, the Android APK's API wrapper, and observing live traffic and not
from any official documentation or source access.

- Core torrent streaming and all P0 desktop playback routes are working.
- HLS transcoding, casting, archive sources, and other P1/P2 features are not yet implemented.
- Behaviour may diverge from the original in untested edge cases.

See the endpoint map below for a full breakdown of what is and isn't implemented yet.

## Replacing the official server

This is meant to be used with my [Stremio fork](https://github.com/Loukious/stremio-shell-ng).
but can very well be used with the original shell as well. To use it, build this project in
release mode (or just download the binaries from the github releases) and rename stremio-service-rs.exe
to stremio-runtime.exe. Then replace the original.

---

## Stremio Service Endpoint Map

This file maps the HTTP contract exposed by the original Stremio streaming server so a new Rust
`StremioService` can replace it without changing the shell/client first.

Sources used:

- `server.js`
- `com/stremio/common/api/StreamingServerApi.java`
- `com/stremio/core/types/resource/Stream.java`

---

## Implementation Status

Legend: ✅ Done · ⚠️ Stub (route exists, returns empty/null) · ❌ TODO

### Runtime Contract
- ✅ Binds to `127.0.0.1:11470`, increments up to `11474` on conflict
- ✅ Prints `EngineFS server started at http://127.0.0.1:<port>` on stdout
- ✅ CORS — all origins, `GET/POST/HEAD/OPTIONS`, `max-age=1728000`
- ❌ HTTPS endpoint on port `12470`

### Core Torrent Routes
- ✅ `GET /favicon.ico`
- ✅ `GET /heartbeat`
- ✅ `GET /settings`
- ✅ `POST /settings`
- ✅ `GET /stats.json` — `?sys=1` loadavg/cpus fields not populated
- ✅ `GET /removeAll`
- ✅ `ALL /create` — hex blob, HTTP URL, and local file path
- ✅ `ALL /{infoHash}/create` — trackers, filters, guessFileIdx, peers
- ✅ `GET /{infoHash}/stats.json`
- ✅ `GET /{infoHash}/remove`
- ✅ `GET /{infoHash}/{idx}/stats.json`
- ✅ `GET /{infoHash}/{idx}` — range, HEAD, `external=1`, `download=1`, `subtitles=`, `tr=`, `f=`
- ✅ `GET /{infoHash}/{idx}/{*filename}` — filename-style stream URL

### Subtitle Routes
- ✅ `GET /subtitles.{ext}` — full proxy with SRT→VTT conversion; `offset=` param not implemented
- ⚠️ `GET /opensubHash` — returns `{ error: null, result: null }`; hash not computed
- ⚠️ `GET /subtitlesTracks` — returns empty tracks; `subsUrl` not fetched or parsed
- ❌ `GET /tracks/:url`

### Probe / HLS
- ⚠️ `GET /probe` — returns null result; no ffprobe invocation
- ⚠️ `GET /hlsv2/probe` — returns null result; no ffprobe invocation
- ❌ `GET /hlsv2/:id/:track.m3u8`
- ❌ `GET /hlsv2/:id/:track/init.mp4`
- ❌ `GET /hlsv2/:id/:track/segment:n.:ext`
- ❌ `GET /hlsv2/:id/burn`
- ❌ `GET /hlsv2/status`
- ❌ `GET /hlsv2/:id/destroy`
- ❌ HLSv2 compat routes (`/:infoHash/:videoId/:playlist`)
- ❌ Legacy HLS routes (`/:first/:second/hls.m3u8`, `stream.m3u8`, segments, etc.)

### System / Info Routes
- ✅ `GET /` — 307 redirect to web UI with `?streamingServer=`
- ✅ `GET /network-info`
- ✅ `GET /device-info`
- ✅ `GET /hwaccel-profiler`
- ⚠️ `GET /get-https` — always returns 500 "Cannot get valid certificate"

### Local Addon
- ✅ `GET /local-addon/manifest.json`
- ⚠️ `GET /local-addon/{*rest}` — stub dispatcher returning empty results

### Casting
- ⚠️ `GET /casting` — returns a static VLC entry; no real device discovery
- ❌ `GET /casting/transcode` / `GET /casting/convert`
- ❌ `GET /casting/:devID`
- ❌ `ALL /casting/:devID/player`

### Everything Else
- ❌ `ALL /proxy/:opts/:pathname*`
- ❌ `GET /yt/:id.json` / `GET /yt/:id`
- ❌ `GET /samples/:key.:container`
- ❌ Archive routes: `/rar`, `/zip`, `/7zip`, `/tar`, `/tgz`
- ❌ `/nzb/*`
- ❌ `/ftp/*`

---

## TODO List

### P0 gaps (affect current desktop playback)

- [ ] `GET /subtitles.{ext}` — implement `offset=<ms>` timestamp shifting
- [ ] `GET /opensubHash` — compute actual OpenSubtitles hash from `videoUrl=`

### P1 (casting, proxying, HLS for non-desktop clients)

- [ ] `GET /probe` — invoke ffprobe, return legacy probe model
- [ ] `GET /hlsv2/probe` — invoke ffprobe, return HLSv2 format+streams+samples model
- [ ] `GET /hlsv2/:id/:track.m3u8` — fMP4 HLS playlist generation
- [ ] `GET /hlsv2/:id/:track/init.mp4` — fMP4 init segment
- [ ] `GET /hlsv2/:id/:track/segment:n.:ext` — fMP4 media segments and VTT subtitle segments
- [ ] `GET /hlsv2/:id/burn` — embedded subtitle burn-in
- [ ] `GET /hlsv2/status` — converter session map
- [ ] `GET /hlsv2/:id/destroy` — tear down converter session
- [ ] HLSv2 compat routes — `/:infoHash/:videoId/:playlist` rewrite into hlsv2 router
- [ ] Legacy HLS routes — `hls.m3u8`, `stream.m3u8`, `stream-q-*.m3u8`, `.ts` segments, `dlna`, `subs-*.m3u8`, `thumb.jpg`
- [ ] `ALL /proxy/:opts/:pathname*` — HTTP proxy with playlist URL rewriting
- [ ] `GET /casting` — real device discovery (Chromecast etc.)
- [ ] `GET /casting/transcode` / `GET /casting/convert` — ffmpeg transcode stream for casting
- [ ] `GET /casting/:devID` — device detail
- [ ] `ALL /casting/:devID/player` — cast player control
- [ ] `GET /subtitlesTracks` — fetch `subsUrl`, parse and return timestamped tracks
- [ ] `GET /tracks/:url` — media track metadata endpoint

### P2 (archive sources, YouTube, HTTPS, samples)

- [ ] `GET /` + HTTPS endpoint on port 12470 + `GET /get-https` returning real cert info
- [ ] `/rar/*`, `/zip/*`, `/7zip/*`, `/tar/*`, `/tgz/*` — archive create/stream routes
- [ ] `/nzb/*` — NZB create/stream
- [ ] `/ftp/*` — FTP create/stream
- [ ] `GET /yt/:id.json` / `GET /yt/:id` — YouTube format resolution via yt-dlp
- [ ] `GET /samples/:key.:container` — bundled AV sample files for hwaccel profiling

---

## Runtime Contract

- Default HTTP listen address is `127.0.0.1:11470`.
- If port `11470` is unavailable, the old server increments up to `11474`.
- Desktop shell expects stdout containing `EngineFS server started at http://127.0.0.1:<port>`.
- Optional HTTPS endpoint listens on `12470` and is advertised by `/get-https`.
- Request bodies are JSON and URL-encoded with an old 3 MB JSON limit.
- CORS is accepted for Stremio origins and localhost. EngineFS also handles preflight `OPTIONS`.
- Streaming responses must support `HEAD` where Express maps it to the `GET` handler.

## APK Client Usage

The Android API wrapper directly uses this subset:

| Method | Path | Inputs | Return |
| --- | --- | --- | --- |
| `GET` | `/:infoHash/:fileIdx/stats.json` | `infoHash`, `fileIdx` path params. If `fileIdx` is absent in the stream model, Android sends `-1`. | JSON `StreamStatistics` or `null`. |
| `GET` | `/opensubHash` | Query `videoUrl=<url>`. | JSON `{ "error": string|null, "result": { "hash": string|null, "size": number|null }|null }`. |
| `GET` | `/subtitles.vtt` | Query `from=<subtitle-url>`, optional `offset=<milliseconds>`. | WebVTT text. |
| `GET` | `/subtitles.srt` | Query `from=<subtitle-url>`, optional `offset=<milliseconds>`. | SRT text. |
| `GET` | Torrent stream URL with `external` query | Built from `/:infoHash/:fileIdx?external=1`. Client disables redirect following and reads `Location`. | `307` redirect to filename URL. |

Android `StreamStatistics` expects at least:

```json
{
  "infoHash": "hex string",
  "peers": 0,
  "queued": 0,
  "unchoked": 0,
  "downloaded": 0,
  "downloadSpeed": 0,
  "streamProgress": 0,
  "streamLen": 0,
  "streamName": ""
}
```

The old server returns more fields. Keep extra fields when convenient, but the fields above are the
client-critical ones.

## Stream Models From APK

The APK `Stream` source types that map to this service:

| Source | Fields | Server endpoint family |
| --- | --- | --- |
| `Tramvai` | `infoHash: string`, `fileIdx: number|null`, `announce: string[]`, `fileMustInclude: string[]` | Torrent EngineFS routes. |
| `Url` | `url: string` | Usually direct playback, HLS/probe, or proxy helpers. |
| `External` | `externalUrl: string|null`, `androidTvUrl: string|null` | Usually no local streaming server needed. |
| `Rar` | `rarUrls: ArchiveUrl[]`, `fileIdx: number|null`, `fileMustInclude: string[]` | `/rar/*` |
| `Zip` | `zipUrls: ArchiveUrl[]`, `fileIdx: number|null`, `fileMustInclude: string[]` | `/zip/*` |
| `Zip7` | `zip7Urls: ArchiveUrl[]`, `fileIdx: number|null`, `fileMustInclude: string[]` | `/7zip/*` |
| `Tar` | `tarUrls: ArchiveUrl[]`, `fileIdx: number|null`, `fileMustInclude: string[]` | `/tar/*` |
| `Tgz` | `tgzUrls: ArchiveUrl[]`, `fileIdx: number|null`, `fileMustInclude: string[]` | `/tgz/*` |
| `Nzb` | `nzbUrl: string`, `servers: string[]` | `/nzb/*` |
| `ArchiveUrl` | `url: string`, `bytes: number|null` | Archive create bodies. |

## Core Torrent Routes

These are mounted at the server root.

### `OPTIONS *` ✅

Handled by CORS middleware (`tower-http` CorsLayer).

Request:

- Header `Origin`.
- Header `Access-Control-Request-Headers` optional.

Response:

- `200` empty body.
- Headers:
  - `Access-Control-Allow-Origin: *`
  - `Access-Control-Allow-Methods: POST, GET, OPTIONS`
  - `Access-Control-Allow-Headers: <request header value or Range>`
  - `Access-Control-Max-Age: 1728000`

### `GET /favicon.ico` ✅

Response:

- `404`
- `Content-Type: application/json`
- Empty body.

### `GET /:infoHash/stats.json` ✅

Response:

- `200 application/json`
- `EngineStats` for the torrent, or `null` if not created.

### `GET /:infoHash/:idx/stats.json` ✅

Inputs:

- `infoHash`: torrent infohash.
- `idx`: torrent file index. `-1` is accepted by clients but old stats only adds stream fields when
  `files[idx]` exists.

Response:

- `200 application/json`
- `EngineStats` plus stream fields when `idx` is a valid file index, or `null`.

### `GET /stats.json` ✅

Inputs:

- Query `sys=1` optional. ⚠️ `sys.loadavg` and `sys.cpus` are not yet populated.

Response:

- `200 application/json`
- Object keyed by infohash. When `sys=1`, includes:

```json
{
  "sys": {
    "loadavg": [0, 0, 0],
    "cpus": []
  },
  "<infohash>": {}
}
```

### `ALL /:infoHash/create` ✅

Creates or returns a torrent engine for a magnet/infohash.

Request body is JSON. Known fields:

```json
{
  "announce": ["tracker-url"],
  "fileMustInclude": ["name-fragment", "/regex/flags"],
  "guessFileIdx": "video-id-or-name",
  "peerSearch": {},
  "connections": 55,
  "uploads": 10,
  "path": "cache-path"
}
```

Behavior:

- `infoHash` is lowercased.
- The whole body is passed into the torrent engine as options.
- If files are known and `fileMustInclude` has values, the first matching file index is returned as
  `guessedFileIdx`.
- If `guessFileIdx` is provided and no match was found, server guesses file index from file list.

Response:

- `200 application/json`
- `EngineStats`.

### `ALL /create` ✅

Creates an engine from a `.torrent` file rather than just an infohash.

Request body:

```json
{
  "blob": "hex-encoded .torrent bytes",
  "from": "http://example/torrent-file.torrent or local path"
}
```

Behavior:

- If `blob` is a string, parse it as hex.
- Else if `from` starts with `http`, fetch it.
- Else read `from` as a local file path.
- Parse torrent metadata and create engine with `{ torrent: parsedTorrent }`.

Responses:

- `200 application/json` with `EngineStats`.
- `500` empty body on parse/read/fetch error.

### `GET /:infoHash/remove` ✅

Destroys one engine.

Response:

- `200 application/json`
- Body `{}`.

### `GET /removeAll` ✅

Destroys all engines.

Response:

- `200 application/json`
- Body `{}`.

### `GET /:infoHash/:idx` ✅

Streams a torrent file.

Inputs:

- `infoHash`: torrent infohash.
- `idx`: numeric file index, filename, or `-1` for guessed selection.
- Header `Range: bytes=start-end` optional.
- Header `enginefs-prio: <number>` optional.
- Query `download=1` optional. Adds `Content-Disposition`.
- Query `external=1` optional. Returns a redirect instead of streaming.
- Query `subtitles=<seconds>` optional. Adds `CaptionInfo.sec`.
- Query `tr=<source>` repeatable. Overrides/extends peer sources.
- Query `f=<filter>` repeatable. Adds file filters.

Responses:

- `307` with `Location: /:infoHash/:filename[?download=1]` when `external` is present.
- `200` full stream when no range.
- `206` partial stream when `Range` is valid.
- `HEAD` returns headers only.

Common response headers:

- `Accept-Ranges: bytes`
- `Content-Type: <mime from filename>`
- `Content-Length: <bytes>`
- `Cache-Control: max-age=0, no-cache`
- `Content-Range: bytes start-end/total` for `206`
- `transferMode.dlna.org: Streaming`
- `contentFeatures.dlna.org: DLNA.ORG_OP=01;DLNA.ORG_CI=0;...`

### `GET /:infoHash/:idx/*` ✅

Same as `GET /:infoHash/:idx`, but `*` can carry a filename path for media players that prefer
stable filenames.

## `EngineStats` Shape

The old server shape is:

```json
{
  "infoHash": "hex string",
  "name": "torrent name",
  "peers": 0,
  "unchoked": 0,
  "queued": 0,
  "unique": 0,
  "connectionTries": 0,
  "swarmPaused": false,
  "swarmConnections": 0,
  "swarmSize": 0,
  "selections": [],
  "wires": [
    {
      "requests": 0,
      "address": "host:port",
      "amInterested": false,
      "isSeeder": false,
      "downSpeed": 0,
      "upSpeed": 0
    }
  ],
  "files": [
    {
      "path": "path/in/torrent.mkv",
      "name": "file.mkv",
      "length": 123,
      "offset": 0
    }
  ],
  "downloaded": 0,
  "uploaded": 0,
  "downloadSpeed": 0,
  "uploadSpeed": 0,
  "sources": {},
  "peerSearchRunning": false,
  "opts": {},
  "streamLen": 123,
  "streamName": "file.mkv",
  "streamProgress": 0,
  "guessedFileIdx": 0
}
```

Notes:

- `wires` is `null` when a specific `idx` stats route is used.
- `streamLen`, `streamName`, and `streamProgress` are only present for a valid `idx`.
- `uploadSpeed` in the old server accidentally mirrors `downloadSpeed`.

## HLSv2 Routes ❌

Mounted at `/hlsv2` unless disabled by environment. Not yet implemented except for the probe stub.

Every route with `:id` creates or reuses a converter. Converter creation query params:

- `mediaURL`: source media URL, required.
- `maxAudioChannels`: integer, optional.
- `forceTranscoding`: truthy query param, optional.
- `profile`: hardware/transcode profile, optional.
- `maxWidth`: optional.
- `videoCodecs`: string or repeated query param.
- `audioCodecs`: string or repeated query param.

### `GET /hlsv2/:id/:track.m3u8` ❌

Inputs:

- `track`: usually `video0`, `audio0`, `subtitle0`, etc.

Response:

- `200 application/vnd.apple.mpegurl`
- Playlist body.
- `500 application/json` on failure:

```json
{
  "error": {
    "code": 10,
    "message": "Failed to read hls playlist: ..."
  }
}
```

### `GET /hlsv2/:id/:track/init.mp4` ❌

Inputs:

- `track` must start with `video` or `audio`.

Response:

- `200 video/mp4`
- fMP4 init segment.
- `500 application/json` with error code `20` on failure.

### `GET /hlsv2/:id/:track/segment:sequenceNumber.:ext` ❌

Inputs:

- `sequenceNumber`: integer.
- `ext`: `m4s` for video/audio tracks, `vtt` for subtitle tracks.

Response:

- `200 video/mp4` for `.m4s`.
- `200 text/vtt` for `.vtt`.
- `500 application/json` with error code `30` on failure.

### `GET /hlsv2/:id/burn` ❌

Inputs:

- Query `url=<subtitle-url>`.
- Query `id=<subtitle-track-id>`.

Response:

- `200` empty body on success.
- `500 application/json` with error code `40` on failure.

### `GET /hlsv2/probe` ⚠️

Inputs:

- Query `mediaURL=<url>`, required.
- Query `samples=1` optional. Attempts to include MP4 or Matroska samples.

Response:

- `200 application/json`

```json
{
  "format": {},
  "streams": [],
  "samples": {}
}
```

Currently returns `{ "error": null, "result": null }`. Real ffprobe invocation not yet wired.

- `500 application/json` on failure. Old code labels the error as `PROBE_FAILED`.

### `GET /hlsv2/status` ❌

Response:

- `200 application/json`
- Object keyed by converter id:

```json
{
  "<id>": {
    "status": {},
    "touched": "date"
  }
}
```

### `GET /hlsv2/:id/destroy` ❌

Response:

- `200` empty body.

### HLSv2 Compatibility Route ❌

These are root routes that rewrite to `/hlsv2`:

```text
GET /:infoHash/:videoId/:playlist
GET /:infoHash/:videoId/:playlist/:HLSSegment
```

Path constraints:

- `infoHash`: 40 hex chars, `file`, or `url`.
- `playlist`: `hls.m3u8`, `videoN.m3u8`, `audioN.m3u8`, `subtitleN.m3u8`.
- `HLSSegment`: `init.mp4`, `segmentN.m4s`, or `segmentN.vtt`.

Rewrite behavior:

- Internal id is `encodeURIComponent(infoHash + "-" + videoId)`.
- `hls.m3u8` becomes `master.m3u8`.
- `mediaURL` is:
  - `file://<videoId>` when `infoHash=file`
  - `<videoId>` when `infoHash=url`
  - `<baseUrlLocal>/<infoHash>/<videoId>` for torrents
- `maxAudioChannels` defaults to `2`.

## Legacy HLS Routes ❌

All use `setHLSFrom`:

- If query `from=<url>` exists, use decoded `from`.
- Else if `first` length is 40, use `<baseUrlLocal>/<first>/<second>`.
- Else if `first` is `file` or `url`, use `second`.

Routes:

| Method | Path | Return |
| --- | --- | --- |
| `GET` | `/:first/:second/hls.m3u8` | HLS master playlist. |
| `GET` | `/:first/:second/master.m3u8` | HLS multi master playlist. |
| `GET` | `/:first/:second/stream.m3u8` | HLS stream playlist. |
| `GET` | `/:first/:second/stream-q-:quality.m3u8` | HLS stream playlist for quality. |
| `GET` | `/:first/:second/stream-:stream.m3u8` | HLS stream playlist for track. |
| `GET` | `/:first/:second/stream-q-:quality/:seg.ts` | MPEG-TS segment. |
| `GET` | `/:first/:second/stream-:stream/:seg.ts` | MPEG-TS segment. |
| `GET` | `/:first/:second/mp4stream-q-:quality.m3u8` | Optional MP4 HLS playlist. |
| `GET` | `/:first/:second/mp4stream-q-:quality/:seg.mp4` | Optional MP4 segment. |
| `GET` | `/:first/:second/dlna` | DLNA MPEG-TS stream. |
| `GET` | `/:first/:second/subs-:lang.m3u8` | Subtitle playlist. |
| `GET` | `/:first/:second/thumb.jpg` | Thumbnail. |
| `GET` | `/thumb.jpg` | Thumbnail, using `from` query when present. |

## Media Probe And Tracks

### `GET /probe` ⚠️

Inputs:

- Query `url=<url>`.
- If the URL does not contain `://`, old server prefixes `baseUrlLocal`.

Response:

- `200 application/json` with probe result.
- `500 application/json` on error, but body is still `JSON.stringify(result)`.

Currently returns `{ "error": null, "result": null }`. Real ffprobe invocation not yet wired.

Typical legacy probe model:

```json
{
  "container": "mkv",
  "duration": 0,
  "bitrate": 0,
  "streams": [
    {
      "codec_type": "video",
      "codec_name": "h264",
      "size": [1920, 1080],
      "stream": 0,
      "default": true,
      "bitrate": 0,
      "fps": 23.976,
      "lang": "eng"
    }
  ]
}
```

### `GET /tracks/:url` ❌

Inputs:

- `url` path param is the media URL.

Response:

- `200 application/json`
- Track data array.
- On error, old server returns `200 []`.

## YouTube Routes ❌

### `GET /yt/:id.json` ❌

Inputs:

- `id`: YouTube video id.

Response:

- `200 application/json` with chosen ytdl format if found.
- `403 application/json` with `{ "err": "message" }` on ytdl error.
- `404 application/json` with `{}` if no playable format URL.

### `GET /yt/:id` ❌

Response:

- `301` redirect to chosen format URL.
- `403` empty body on ytdl error.
- `404` empty body if no playable format URL.

## Subtitle Routes

### `GET /subtitlesTracks` ⚠️

Inputs:

- Query `subsUrl=<url>`.

Response:

- `200 application/json` or `500 application/json`.

```json
{
  "error": null,
  "result": {
    "tracks": [
      {
        "startTime": "date/string",
        "endTime": "date/string",
        "text": "caption"
      }
    ]
  }
}
```

Currently returns empty tracks regardless of `subsUrl`. Fetching and parsing `subsUrl` not yet implemented.

### `GET /opensubHash` ⚠️

Inputs:

- Query `videoUrl=<url>`.

Response:

- `200 application/json` or `500 application/json`.

```json
{
  "error": null,
  "result": {
    "hash": "opensubtitles hash",
    "size": 123
  }
}
```

Currently returns `{ "error": null, "result": null }`. Hash computation not yet implemented.

### `GET /subtitles.:ext` ✅

Inputs:

- `ext`: `vtt` or `srt`.
- Query `from=<subtitle-url>`, required.
- Query `offset=<milliseconds>`, optional. ⚠️ Offset shifting not yet implemented.

Response:

- `200 text/plain-ish` subtitle body.
- `500` empty body on errors or empty track list.

VTT starts with:

```text
WEBVTT

0
HH:mm:ss.SSS --> HH:mm:ss.SSS
Text
```

SRT uses:

```text
0
HH:mm:ss,SSS --> HH:mm:ss,SSS
Text
```

## System And Settings Routes

### `GET /network-info` ✅

Response:

- `200 application/json`

```json
{
  "availableInterfaces": ["192.168.1.10"]
}
```

- `500 text/plain` with error message.

### `GET /device-info` ✅

Response:

- `200 application/json`

```json
{
  "availableHardwareAccelerations": []
}
```

- `500 text/plain` with error message.

### `GET /settings` ✅

Response:

- `200 application/json`

```json
{
  "options": {},
  "values": {},
  "baseUrl": "http://127.0.0.1:11470"
}
```

### `POST /settings` ✅

Request body:

- JSON object with partial setting values.

Response:

- `200 application/json`

```json
{
  "success": true
}
```

### `GET /heartbeat` ✅

Response:

- `200 application/json`

```json
{
  "success": true
}
```

### `GET /get-https` ✅

Always returns 500. Real certificate provisioning is P2.

Inputs:

- Query `ipAddress=<ip>`.
- Query `authKey=<key>`.

Response:

- `200 application/json`

```json
{
  "ipAddress": "192.168.1.10",
  "domain": "local.strem.io",
  "port": 12470
}
```

- `500 text/plain` with body `Cannot get valid certificate`.

### `GET /hwaccel-profiler` ✅

Response:

- `200 application/json` with profile array.
- `500 text/plain` with body `No viable hardware acceleration profiles detected`.

### `GET /` ✅

Requires `Host` header.

Response:

- `307` redirect to configured web UI with query `streamingServer=<encoded current server URL>`.

## Proxy Routes ❌

Mounted at `/proxy`.

### `ALL /proxy/:opts/:pathname(*)?` ❌

Inputs:

- `opts`: querystring encoded in a path segment.
- `pathname`: optional target path.
- Original query string is forwarded to upstream.

Option keys:

- `d`: destination base URL, required, such as `https://example.com`.
- `h`: destination request header override. Repeatable. Format `Name:Value`.
- `r`: response header override. Repeatable. Format `Name:Value`.

Request headers copied upstream:

- `accept`
- `accept-encoding`
- `accept-language`
- `connection`
- `transfer-encoding`
- `range`
- `if-range`
- `user-agent`

Response headers copied back:

- `accept-ranges`
- `content-type`
- `content-length`
- `content-range`
- `connection`
- `transfer-encoding`
- `last-modified`
- `etag`
- `server`
- `date`

Behavior:

- Follows up to 5 redirects.
- For `.m3u`, `.m3u8`, or MPEGURL content, removes `content-length`, sets
  `accept-ranges: none`, ensures chunked transfer, and rewrites playlist URLs through `/proxy`.

Response:

- Upstream status and body, possibly rewritten for playlists.

## Local Addon Routes

Mounted at `/local-addon`.

### `GET /local-addon/manifest.json` ✅

Response:

- `200 application/json; charset=utf-8`
- Addon manifest JSON.

### `GET /local-addon/:resource/:type/:id/:extra?.json` ✅

Inputs:

- `resource`: one of the registered addon resources, typically `catalog`, `meta`, `stream`, or `subtitles`.
- `type`: media type.
- `id`: resource id.
- `extra`: optional querystring-like segment parsed into an object.

Response:

- `200 application/json; charset=utf-8` with handler result.
- `500` with `{ "err": "handler error" }` on handler error.

## Static Sample Routes ❌

The old server registers one route for each bundled AV sample:

```text
GET /samples/:key.:container
```

Response:

- `200`
- `Content-Type` from sample metadata.
- Binary sample body.

These are mainly used by `/hwaccel-profiler` to test HLSv2 hardware acceleration.

## Archive Routes ❌

Archive routes share a common create/stream pattern. They are mounted at:

- `/rar`
- `/zip`
- `/7zip`
- `/tar`
- `/tgz`

Archive create body accepts arrays of either objects or tuples:

```json
[
  { "url": "https://host/file.r00", "bytes": 123 },
  ["https://host/file.r01", 456],
  "https://host/file.r02"
]
```

For non-POST create, the old server expects `?lz=<lz-string encoded json>` where decoded JSON is:

```json
{
  "urls": [{ "url": "https://host/file.rar", "bytes": 123 }],
  "fileMustInclude": ["mkv"],
  "maxFiles": 20,
  "fileIdx": 0
}
```

The generated key is `sha256(lz)` for `lz` create.

### `POST /rar/create/:createKey` ❌
### `ALL /rar/create` ❌

Response:

- POST: `200 application/json` with `{ "key": "<key>" }`.
- Non-POST: `302 Location: /rar/stream?key=<sha256>&o=<encoded-options>`.
- `500 text/plain` on malformed data.

### `GET /rar/stream` ❌

Inputs:

- Query `key=<key>`, required unless direct URL query is supported by parser.
- Query `o=<json options>`, optional.
- Header `Range` optional.

Options:

```json
{
  "fileMustInclude": ["mkv"],
  "maxFiles": 20,
  "fileIdx": 0
}
```

Response:

- `204` headers only for `HEAD`.
- `200` full stream.
- `206` range stream.
- `500` on parser/key errors.

### `POST /zip/create/:createKey` ❌
### `ALL /zip/create` ❌
### `GET /zip/stream` ❌

Same create contract as `/rar`.

Stream behavior:

- Supports range for uncompressed entries by mapping the inner file offset.
- If the zip entry is compressed, only full-stream or `bytes=0-` style access works.
- Bad unsupported range can return `405`.
- Invalid range can return `416`.

### `POST /7zip/create/:createKey` ❌
### `ALL /7zip/create` ❌
### `GET /7zip/stream` ❌

Same as `/rar`, using 7zip parser.

### `POST /tar/create/:createKey` ❌
### `ALL /tar/create` ❌
### `GET /tar/stream` ❌

Same create contract as `/rar`.

Stream behavior:

- Supports byte ranges by mapping tar entry offset.
- Invalid range can return `416`.

### `POST /tgz/create/:createKey` ❌
### `ALL /tgz/create` ❌
### `GET /tgz/stream` ❌

Same create contract as `/rar`.

Stream behavior:

- Usually non-seekable.
- `Accept-Ranges: none`.
- Only `bytes=0-` or full-file style ranges are accepted.
- Other ranges return `405`.

## NZB Routes ❌

Mounted at `/nzb`.

### `POST /nzb/create/:createKey` ❌
### `ALL /nzb/create` ❌

POST body:

```json
{
  "servers": ["nntp://user:pass@host:119/20"],
  "nzbUrl": "https://host/file.nzb",
  "nzbUrls": ["https://host/file1.nzb", "https://host/file2.nzb"]
}
```

Non-POST query:

- `lz=<lz-string encoded json>`, decoded shape is the same as POST body.

Behavior:

- `servers` must be a non-empty array.
- Either `nzbUrl` or `nzbUrls` is required.
- Multiple NZB URLs are tried in chunks of five.
- The server detects direct video files or archive sets inside NZB metadata.
- If an archive is detected, it internally creates the matching archive key and redirects there.

Responses:

- POST success: `200 application/json` with `{ "key": "<key>" }`.
- Non-POST success: `302 Location: <stream path>`.
- `500 text/plain` on malformed data or failed NZB checks.

### `GET /nzb/stream/:key/:fileName` ❌

Streams one file from the initialized NZB session.

Inputs:

- Header `Range` optional.

Response:

- `200` or `206` stream.
- `500` on key/file/session error.

### `GET /nzb/stream` ❌

Inputs:

- Query `key=<key>`.

Response:

- Redirects to the initialized stream path.
- `500` if key has no stream.

## FTP Routes ❌

Mounted at `/ftp`.

### `POST /ftp/create/:createKey` ❌
### `ALL /ftp/create` ❌
### `ALL /ftp/:fileName` ❌

POST body:

```json
{
  "ftpUrl": "ftp://user:pass@host:21/path/file.mkv"
}
```

Non-POST query:

- `lz=<lz-string encoded json>`, decoded shape:

```json
{
  "ftpUrl": "ftp://user:pass@host:21/path/file.mkv"
}
```

Behavior:

- Supports `ftp` and `ftps`.
- Checks last modified, size, MIME, and FTP `REST` support.
- Creates a stream URL `/ftp/stream/:key/:filename`.

Responses:

- POST success: `200 application/json` with `{ "key": "<key>" }`.
- Non-POST success: `302 Location: /ftp/stream/:key/:filename`.
- `500` on malformed data or connection error.

### `GET /ftp/stream/:key/:fileName` ❌

Inputs:

- Header `Range` optional.

Response:

- `200` full stream.
- `206` range stream.
- `405` if range is requested but the FTP server does not support seeking, except full `bytes=0-`.
- `HEAD` returns headers only.

### `GET /ftp/stream` ❌

Inputs:

- Query `key=<key>`.

Response:

- Redirects to initialized `/ftp/stream/:key/:fileName`.
- `500` on missing key.

## Casting Routes

Mounted at `/casting/` on non-Android platforms when casting is enabled.

### `GET /casting/` ⚠️

Response:

- `200 application/json; charset=utf8`
- Array of discovered devices.

Currently returns a static VLC stub. Real device discovery not yet implemented.

### `GET /casting/transcode:ext?` ❌
### `GET /casting/convert:ext?` ❌

Inputs:

- Query `video=<url>`, required.
- Query `fmp4=1`, optional. Uses fragmented MP4 instead of MKV.
- Query `audioTrack=<id>`, optional.
- Query `time=<seconds>`, optional.
- Query `subtitles=<url>`, optional.
- Query `subtitlesDelay=<seconds>`, optional.
- Query `flagRe=1`, optional ffmpeg pacing flag.
- Header `getmediainfo.sec` optional. Adds `MediaInfo.sec`.

Response:

- `200` chunked stream.
- `400` body `provide ?video` when missing `video`.

### `GET /casting/:devID` ❌

Response:

- `200 application/json; charset=utf8` with device object.
- `404 text/plain` body `Device not found`.

### `ALL /casting/:devID/player` ❌

Inputs can be query params or request body:

- `formats`: calls `protocolsGet`.
- `audioTrack=<id>`: switch audio.
- `volume=<number>`: set volume.
- `time=<milliseconds>`: seek.
- `subtitlesSrc=<url>`: set subtitles.
- `subtitlesDelay=<milliseconds>`: set subtitle delay.
- `subtitlesSize=<number>`: set subtitle font size.
- `source=<url>`: play source. Empty `source` closes player.
- `stop`: stop.
- `paused`: truthy pauses, falsy resumes.

Response:

- `200 application/json; charset=utf8` with media status or `{}`.

Media status shape:

```json
{
  "audio": [],
  "audioTrack": null,
  "volume": 100,
  "time": 0,
  "paused": false,
  "state": 5,
  "length": 0,
  "source": null,
  "subtitlesSrc": null,
  "subtitlesDelay": 0,
  "subtitlesSize": 2
}
```

## Endpoint Collisions To Preserve

Route order matters in the old server:

- HLSv2 compatibility routes are registered before legacy HLS routes.
- Torrent routes are also root-level dynamic routes. Static routes like `/settings`, `/heartbeat`,
  `/opensubHash`, `/proxy`, `/local-addon`, `/hlsv2`, and archive mounts must win over torrent
  `/:infoHash/:idx`.
- `/:infoHash/:idx/*` is used by filename-style stream URLs, especially after `external=1`.

For Rust, use explicit static routes first, constrained regex routes second, and torrent catch-all
routes last.

## Recommended Rust Service Shape

- HTTP layer: `axum` or `actix-web`.
- Torrent backend: prefer a mature libtorrent binding/process boundary over reimplementing BitTorrent
  and uTP. The old JavaScript failure mode is mostly in uTP/peer connectivity, not the HTTP API.
- Keep the HTTP endpoint JSON stable while swapping internals.
- Start with P0 routes and return realistic stub values for P1/P2 only if the current client does not
  call them.
- Build conformance tests with captured requests:
  - stats polling
  - torrent stream range and no-range
  - `external=1` redirect
  - subtitles proxy
  - settings and heartbeat
