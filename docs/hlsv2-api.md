# HLSv2 media probe contract

The runtime implements the media probe used by current Stremio Web's
`@stremio/stremio-video` streaming-server wrapper. This is separate from the
legacy `GET /probe?url=...` endpoint. The browser calls the HLSv2 probe to decide
whether a container, video codec, audio codec, and channel count can play directly.
The wrapper can request conversion when the media is unsupported, has embedded
subtitles or multiple audio tracks, or `forceTranscoding` is enabled. Native MPV
playback reads metadata through MPV itself.

## Request and response

`GET /hlsv2/probe?mediaURL=<percent-encoded URL>` accepts one HTTP(S) URL or
relative streaming path. Relative paths use the runtime's advertised local URL,
including a dynamically selected port. Signed query strings remain in the input
URL. Local file paths and other protocols are rejected.

A successful response is HTTP 200 with the following direct JSON object:

```json
{
  "format": { "name": "matroska,webm", "duration": 7200.125 },
  "streams": [
    {
      "id": 0, "index": 0, "track": "video", "codec": "h264",
      "streamBitRate": 4000000, "streamMaxBitRate": 0,
      "startTime": 0, "startTimeTs": 0, "timescale": 1000,
      "width": 1920, "height": 1080, "frameRate": 23.976023976,
      "numberOfFrames": null, "isHdr": false, "isDoVi": false,
      "hasBFrames": true, "formatBitRate": 5000000,
      "formatMaxBitRate": 0, "bps": 0, "numberOfBytes": 0,
      "formatDuration": 7200.125
    },
    {
      "id": 0, "index": 2, "track": "audio", "codec": "aac",
      "streamBitRate": 192000, "streamMaxBitRate": 0,
      "startTime": 0, "startTimeTs": 0, "timescale": 48000,
      "sampleRate": 48000, "channels": 2, "channelLayout": "stereo",
      "title": "English", "language": "eng"
    }
  ],
  "samples": {}
}
```

`format.name` retains FFprobe's comma-separated container names; it is not
normalized to the legacy `mkv` name. Durations and start positions are seconds.
Each stream has an `id` counting only streams of its own type and an `index`
retaining its global input index. For example, the first audio stream can have
`id: 0` and `index: 2`; its route is `audio0`, and its FFmpeg mapping is `0:2`.
Unknown stream types remain in the result.

Video streams include resolution, frame rate, frame count, bitrate information,
HDR/Dolby Vision flags, and B-frame presence. Audio streams include sample rate,
channel count/layout, title, and language. Subtitle streams include title and
language. Attachments include `filename`, `mimeType`, and extradata `size`.
Missing numeric metadata becomes JSON null where the upstream contract permits
it; absent bitrate fields default to zero. Type-specific fields appear only on
the relevant stream type.

## Optional sample timing metadata

`samples=1` requests actual MP4 timing tables or Matroska cue points. The runtime
reads metadata with HTTP byte ranges and skips media payloads. It supports MP4
`moov` atoms at the beginning or end of large files, 64-bit atom sizes, edit lists,
composition offsets, and sync-sample tables. Matroska SeekHead references can
locate cues near EOF without downloading clusters.

The `samples` object is keyed by global input index:

```json
{
  "0": {
    "type": "video", "timescale": 90000, "sampleDuration": 3750,
    "samples": [{ "key": true, "pts": 0, "dts": 0, "duration": 3750 }]
  }
}
```

Times are integer ticks in the declared timescale, not seconds. Matroska cue
entries contain `key` and `pts`; their `dts` and `duration` fields are omitted.
Unsupported containers return an empty samples object. MP4 files without timing
entries or Matroska files without cues can return empty arrays; these do not
provide sufficient keyframe evidence for arbitrary video-copy seeking.

Sample extraction has a 60-second deadline, at most 256 HTTP reads, a 32 MiB
limit for an individual metadata region, a 64 MiB transfer budget, and at most
one million timing entries. Origins without byte ranges support only bounded
metadata at the front of the response. A malformed or unavailable requested
sample table returns a probe error rather than invented keyframe positions.

## Execution and errors

FFprobe is selected through `STREMIO_FFPROBE_PATH`, beside the runtime executable,
then on PATH. HLS probes share the two-process concurrency limit with legacy
probes and have a 60-second deadline, 4 MiB stdout limit, and 64 KiB stderr limit.
Cancellation and failures terminate and reap the child process. Successful basic
probe results are cached for five minutes with at most three media URLs.

Failures use `{"error":{"code":100,"message":"Failed to probe media: ..."}}`.
Invalid input returns HTTP 400, concurrent-probe rejection returns 503, deadlines
return 504, and process/metadata failures return 500. The upstream error table
names probe failure code 100; its route contains a `PROBE_FAILED` typo that omits
the code. The runtime uses the intended code. Code 20 belongs to initialization
segment failure.

The legacy `/probe` keeps its existing response model, execution limits, and
error envelope. This HLSv2 endpoint supplies the browser metadata contract;
playlist and segment conversion use the separate HLS service.

## Reference

The supplied upstream `server.js` has SHA-256
`B368D339709584879F024FA5FABE499E583B25881B2375A6192625C5277AA5B0`.
The rich FFprobe mapper is at lines 39508–39583, the HLSv2 probe route at
77232–77255, the MP4 timing parser at 79314–79353, and the Matroska cue parser at
79263–79307. Stremio Web's consumer is
[withStreamingServer](https://github.com/Stremio/stremio-video/blob/7a7462bedfa51dc8303fbc4f59f16e03d0badb85/src/withStreamingServer/withStreamingServer.js).

REA static evidence is `ev_07799a85cf9f6071b777d8f9f7d8af8d4a82ca0168ad610f224c98132fef06ad`.
Its graph has partial semantic coverage after a worker memory limit; the contract
above is supported by focused source inspection. Static evidence does not record
execution of the upstream server.
