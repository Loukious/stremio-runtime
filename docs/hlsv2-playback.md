# HLSv2 playback

The runtime implements the HLSv2 conversion routes used by current Stremio Web. Each converter exposes one video rendition, separate audio renditions, and supported text subtitle renditions. Video and audio decisions are independent: compatible video can be remuxed while DTS, AC3, PCM, multichannel audio, or another incompatible audio track is converted to AAC.

Media must be HTTP(S), seekable, have a known positive duration, and be no longer than seven days. Relative URLs resolve against the advertised runtime address, including its selected port. Absolute signed source URLs retain their original bytes. Live streams and hardware conversion profiles are unsupported. The source server must provide reliable byte ranges for efficient seeking; playback does not preconvert the entire file.

## Converter options

Create a converter by requesting `/hlsv2/{id}/master.m3u8?mediaURL=...`. IDs contain at most 128 ASCII letters, digits, hyphens, or underscores. Rendition URLs preserve the creation query, so a later segment request can recreate an expired converter. Reusing an active ID retains its original source and options; destroy the ID before changing the source.

| Query | Behavior |
| --- | --- |
| `mediaURL` | Required HTTP(S) source, or a relative runtime proxy/torrent URL. |
| `videoCodecs` | Repeatable compatible codec names; explicitly allow `h264` or `hevc` to request video remuxing. |
| `audioCodecs` | Repeatable names; `aac` allows AAC remuxing when channel count and timing are compatible. |
| `maxAudioChannels` | Positive integer up to 32; default 2. Converted audio is downmixed to this limit. |
| `maxWidth` | Positive integer up to 7680; reduced to an even width while preserving aspect ratio. Required scaling converts video. |
| `forceTranscoding` | A nonempty value converts both video and audio. Omit it to allow copying. |
| `profile` | Empty, `none`, `software`, and `cpu` use software conversion. Other profiles return a creation error. |

## Copying and conversion

H264 copying supports B frames and variable frame rate when bounded keyframe metadata is available. MP4 uses its complete sample tables to verify GOP presentation boundaries. Matroska uses cues plus a bounded later-key-packet check; the first VCL NAL must be an IDR picture. Every returned copied video segment also checks its independent start. An unexpected later non-IDR GOP returns a conversion error requiring `forceTranscoding`, rather than returning dependent frames that cannot decode after a seek. Missing/slow metadata, open GOPs detected during creation, scaling, or incompatible codecs select software H264 conversion.

HEVC copying is restricted to MP4 with validated closed-GOP sample timing. The output sample entry is `hvc1`, and the caller must explicitly include `videoCodecs=hevc`. HEVC Matroska currently converts to H264 because cues do not establish safe HEVC CRA/leading-picture boundaries. Browser HEVC playback also depends on the browser and installed decoder.

Copied video clips use actual packet presentation timestamps to discard packets outside the segment. Complete MP4 tables additionally provide exact sample counts. Matroska never infers VFR sample counts from nominal frame rate. A canonical initialization header and the MP4 edit-list offset preserve B-frame reorder timing across out-of-order seeks. The initial copied-video request may read the first short GOP to establish that header, then reads the requested GOP directly.

Converted video uses libx264, baseline H264, no B frames, an integer source frame rate capped at 60 fps, and a fixed 90 kHz track timebase. Converted audio uses AAC at 48 kHz. Audio begins on the source timeline: delayed starts become silence, and audio ending before the video is padded with silence through the advertised VOD duration. AAC is copied only when a complete packet table proves it covers the VOD timeline and there is no material start offset; otherwise it is converted.

Playlists are HLS version7 VOD, start at sequence 1, include a short final segment and `EXT-X-ENDLIST`, and support arbitrary segment order. Converted tracks use approximately four-second segments. Copied video follows keyframes, with a maximum twelve-second segment. Text subtitles use absolute video timestamps and clip overlapping cues to each segment.

## Routes

| Route below `/hlsv2/{id}/` | Result |
| --- | --- |
| `master.m3u8` | Master playlist with audio and text subtitle groups. |
| `video0.m3u8`, `audioN.m3u8`, `subtitleN.m3u8` | Track playlists; `N` is a per-type ordinal, independent of the source stream index. |
| `video0/init.mp4`, `audioN/init.mp4` | Fragmented-MP4 initialization header. |
| `video0/segmentN.m4s`, `audioN/segmentN.m4s` | Media fragment; sequence starts at 1. |
| `subtitleN/segmentM.vtt` | WebVTT cues on the absolute video timeline. |
| `source/subtitle/N.ass` | Embedded ASS/SSA source text. |
| `source/attachment/N` | Embedded attachment bytes and source MIME type when valid. |
| `burn?url=...` | Burn an external HTTP(S) text subtitle into transcoded video. |
| `burn?id=N` | Burn embedded text subtitle ordinal `N`, including embedded ASS fonts. |
| `burn` | Clear subtitle burning. |

`GET /hlsv2/status` reports sessions, probe values, converter copy decisions, cached segment numbers, and active process count. `GET /hlsv2/{id}/destroy` cancels the converter, stops subprocesses, releases caches, and returns an empty 200 response. Probe behavior is documented in [hlsv2-api.md](hlsv2-api.md).

Burning requires transcoded video. A converter already copying video rejects burn; recreate it with `forceTranscoding=1` first. Changing burn invalidates the video initialization header and media cache. Subtitle filters use absolute source time even when seeking, so a cue at eight seconds appears in a directly requested third segment. ASS/SSA source extraction can read the full subtitle track, but output and process deadlines remain bounded. Bitmap subtitle codecs are omitted from subtitle renditions and are unsupported for burning.

FFmpeg must include libass and its font dependencies. Packaged fallback fonts are linked/copied into the owned per-converter font directory alongside embedded fonts. The child process uses adjacent `fonts.conf` when present and `FONTCONFIG_FILE` is not already set. Temporary subtitle/font files are deleted when replaced or when the converter is destroyed/expired.

## Bounds and errors

The service allows two converters and two concurrent FFmpeg processes. Idle converters expire after 120 seconds. A session stores at most 12 tracks, eight media segments per track, and 64 MiB of media cache; each initialization header is limited to 1 MiB. Process stdout is capped at 64 MiB for segments and 16 MiB for ASS/attachments; external or extracted burn text is capped at 4 MiB. Embedded fonts allow eight files totaling 32 MiB, and packaged fallback fonts allow 32 files totaling 32 MiB. Stderr is capped at 256 KiB and is not included in public error messages. Process-slot waits are limited to 30 seconds and each FFmpeg operation to 120 seconds. Requests, converter destruction, and runtime shutdown own/cancel their subprocesses.

Invalid track/segment routes return 404. Capacity exhaustion returns 503. Other failures return 500 with `{ "error": { "code": number, "message": string } }`.

| Code | Operation |
| --- | --- |
| 1 | Create converter/options |
| 10 | Read playlist |
| 20 | Read initialization segment |
| 30 | Read media segment |
| 40 | Burn subtitles |
| 50 | Read ASS/SSA source |
| 60 | Read attachment source |
| 100 | Probe media |
| 200 | Read status |

## Native validation

Standard `cargo test --locked` runs without external media tools. Actual HTTP-range fixtures, decode/seek tests, delayed/short audio, B-frame/VFR copying, HEVC+AC3 copying/conversion, subtitle source/font/burn tests, open-GOP fallback, and process cancellation/output limits are opt-in:

```sh
cargo test --locked media_hls -- --include-ignored --test-threads=1
```

Set `STREMIO_FFMPEG_PATH` and `STREMIO_FFPROBE_PATH` to exercise packaged tools. Fixture subprocesses honor those paths. The tiny checked-in HEVC input allows testing HEVC copying without a libx265 encoder in the package.
