# Real Stremio Web playback verification

The API fixture generates an unsupported MPEG-4/PCM Matroska movie with two
audio tracks and embedded ASS subtitles, serves it through an HTTP origin with
Range support, and launches an isolated runtime on an ephemeral port. It checks
the probe contract, signed URL encoding, multiple playlists, stereo downmixing,
out-of-order segments, FFmpeg decoding, subtitle extraction, and converter cleanup.
It never uses an installed Stremio application's settings or cache.

Run from the repository root with Python 3.9+, FFmpeg, and FFprobe on `PATH`:

```sh
cargo build --locked
python scripts/verify-web-hls.py --runtime target/debug/stremio-runtime
```

Use `stremio-runtime.exe` on Windows. `--ffmpeg` and `--ffprobe` accept explicit
executable paths. Results and fixture media are stored in `target/web-hls-verification`.
Release CI runs this check against each platform's packaged runtime and media tools.

Add `--copy-video` to generate H.264 with B-frames and verify that video is copied
while six-channel PCM audio is converted to stereo AAC. The fixture checks the
converter's reported copy mode in addition to decoding its output.

For a browser check, first build the harness using the same player library as
current Stremio Web, pinned to `@stremio/stremio-video` 0.0.103:

```sh
cd tests/web
npm ci --ignore-scripts --no-audit --no-fund
npm run build
cd ../..
python scripts/verify-web-hls.py --runtime target/debug/stremio-runtime --serve-browser
```

Open the `browserFixture` URL printed after the API checks. The harness forces
the streaming-server HLS path, observes playback properties, and exposes
`window.hlsVerification` for inspection in the browser console:

```js
hlsVerification.state                 // time/duration are milliseconds
hlsVerification.seek(16)              // seek in seconds
hlsVerification.audio('EMBEDDED_1')   // switch to the second audio track
hlsVerification.player.dispatch({
    type: 'setProp', propName: 'selectedSubtitlesTrackId', propValue: 'EMBEDDED_0'
})
hlsVerification.events                // errors, track events, natural completion
hlsVerification.destroy()
```

Check that time advances, seeking resumes playback, the second audio track stays
selected, embedded subtitle cues appear, and completion happens at the source
duration. The optional `/subtitleSignature` request and favicon can return 404;
neither prevents playback. Close the harness and stop the fixture after testing.
