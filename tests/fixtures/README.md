`hls-hevc-closed-gop.mp4` is generated media for the opt-in HLS tests. It contains 9.25 seconds of 160×90 HEVC Main video at 24 fps with B frames and closed four-second GOPs, plus two-channel AC3 audio. No external footage is included. The checked-in file allows testing the bundled HEVC decoder/remuxer without requiring a libx265 encoder in release archives.

Generate with FFmpeg built with libx265:

```sh
ffmpeg -hide_banner -loglevel error -nostdin -y \
  -f lavfi -i 'testsrc2=size=160x90:rate=24:duration=9.25' \
  -f lavfi -i 'sine=frequency=440:sample_rate=48000:duration=9.25' \
  -c:v libx265 -preset fast -crf 32 \
  -x265-params 'pools=2:frame-threads=2:keyint=96:min-keyint=96:scenecut=0:open-gop=0:bframes=3:log-level=error' \
  -tag:v hvc1 -c:a ac3 -b:a 192k -ac 2 -movflags +faststart \
  tests/fixtures/hls-hevc-closed-gop.mp4
```
