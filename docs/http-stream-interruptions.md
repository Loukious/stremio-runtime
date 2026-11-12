# HTTP playback interruptions

Investigated on 2026-11-12 after reports of large debrid videos ending at random
positions. The reported playback URL goes directly to the debrid/CDN host.

## Playback path and observed failure

The shell sends a direct HTTP(S) URL to MPV. MPV downloads the media itself;
the runtime proxy and torrent engine do not carry that response. Updating rqbit
therefore does not change direct HTTP playback.

A controlled, local HTTP source reproduced an incomplete response being treated
as normal EOF by MPV. One fixture advertises the full faststart MP4 in
`Content-Range` and ends a chunked `206` after a short prefix. The portable fixture
instead returns `200` with a correctly declared length for only the file prefix;
the MP4 metadata still declares the full duration. Using the installed libmpv and
the shell's existing reconnect options, both cases report known duration and
seekability but end with EOF and no error, without reconnecting.
For the portable 40-second video, this occurred around 4.9 seconds. Reloading the
identical URL, waiting for MPV's FileLoaded event, and then seeking to the last
position with `seek <position> absolute` reached completion around 39.8 seconds.
This follows the shell's recovery command sequence. The percent-encoded query
string remained identical on both requests. The fixture passed with both the
installed libmpv and the November 8 build (`mpv v0.42.0-5-g55d3feb45`).

MPV's seekability and duration become unavailable when the file unloads, so the
shell must retain their last valid values through the EndFile event. The native
fixture also observed an unavailable `time-pos` notification before EndFile.

Valid bounded partial responses and transport closures that truncate a declared
`Content-Length` recovered with MPV's existing reconnect options. They did not
reproduce this clean-EOF failure. The controlled reproduction establishes a
failure mechanism and recovery strategy; the actual debrid provider's response
at the time of an interruption has not been captured.

## Shell recovery

The companion shell change handles a premature EOF or error for a loaded,
seekable HTTP(S) video when duration and position are finite, duration exceeds
30 seconds, and the position is more than 10 seconds before the expected end.
It reloads the original URL byte-for-byte and resumes at the last position,
allowing up to three recovery attempts per file. Signed paths and query strings
are preserved. Failed or exhausted direct recovery emits the existing playback
error event. Normal completion, user stops, and pending file replacements keep
their existing behavior.

The event adapter also preserves MPV's EndFile reason when MPV includes a negative
error code, allowing both recovery and terminal playback errors to reach the
normal event handler. Local torrent recovery retains its existing one-reload
policy. Unseekable streams do not use the new direct-URL recovery.

The direct playback change requires a rebuilt shell; replacing only the runtime
executable does not install it.

## Separate runtime proxy fix

The runtime proxy previously applied a 90-second Reqwest timeout to the whole
response, including a streaming media body. A proxy connection could therefore
fail after 90 seconds even while delivering data. The proxy client now has a
15-second connection timeout, a 90-second response-header timeout per redirect
hop, and a separate 90-second deadline for buffered playlist bodies. Streaming
media bodies have no total deadline. Header and playlist timeouts return HTTP 504.

Stream errors remain errors. Logs identify the origin and byte counts without
including a signed path or query. Advertised byte counts use `u64`; range offsets
above 4 GiB and a 9 GB HEAD response are covered by regression fixtures.
Compressed playlist rewriting continues to use Reqwest's existing decompression.

## Reproducing the native behavior

Run the portable fixture with Python 3, FFmpeg, and a libmpv shared library:

```powershell
python scripts/reproduce-http-eof.py --mpv-library <path-to-libmpv-2.dll> --ffmpeg <path-to-ffmpeg.exe> --output-dir target/http-eof-repro
```

It generates its own media and uses a loopback origin and an isolated headless
MPV instance. It does not need a debrid URL or modify the installed application.
The output records MPV version, property/event observations, request paths, the
FileLoaded-triggered seek command, and baseline versus reload results. This fixture
verifies the native reload strategy; shell regression tests cover the eligibility
and controller state transitions. `--duration` must exceed the shell's 30-second
recovery gate; the default fixture lasts 40 seconds.

## Validation

The runtime's locked check and formatting checks passed. All 33 runtime tests
passed, including six HTTP framing/deadline fixtures. Clippy completed with
pre-existing warnings. The static-CRT Windows x64 release build passed.

The runtime dependency is pinned to librqbit 9.1.0 from the rebased fork revision
`8d44d633fe30de2697470b7a21d686b97669f9d2`. Its Cargo lock uses the published
revision rather than local dependency patches.
