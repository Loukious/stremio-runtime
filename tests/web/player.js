import StremioVideo from '@stremio/stremio-video';

// Exercise the same player library and HLS wrapper as current Stremio Web.
const events = [];
const state = {};
const player = new StremioVideo();
window.hlsVerification = { events, state, player, ready: false };
for (const event of ['error', 'ended', 'implementationChanged', 'audioTrackLoaded', 'subtitlesTrackLoaded']) {
    player.on(event, (...args) => events.push({ event, args: args.map(arg =>
        arg instanceof Error ? arg.message : arg) }));
}
for (const event of ['propValue', 'propChanged']) {
    player.on(event, (name, value) => {
        state[name] = value;
        if (['loaded', 'duration', 'audioTracks', 'selectedAudioTrackId', 'buffering'].includes(name)) {
            events.push({ event, name, value });
        }
    });
}

async function start() {
    const config = await fetch('/test-config.json').then(response => response.json());
    player.dispatch({
        type: 'command', commandName: 'load', commandArgs: {
            stream: { url: config.mediaURL },
            streamingServerURL: config.runtimeURL,
            forceTranscoding: config.forceTranscoding,
            platform: 'web',
            autoplay: true,
        },
    }, { containerElement: document.getElementById('player') });
    for (const propName of ['loaded', 'time', 'duration', 'buffering', 'audioTracks',
        'selectedAudioTrackId', 'subtitlesTracks', 'selectedSubtitlesTrackId']) {
        player.dispatch({ type: 'observeProp', propName });
    }
    player.dispatch({ type: 'setProp', propName: 'muted', propValue: true });
    const video = document.querySelector('video');
    if (video) video.muted = true;
    window.hlsVerification.ready = true;
}
window.hlsVerification.seek = seconds => player.dispatch({
    type: 'setProp', propName: 'time', propValue: seconds * 1000,
});
window.hlsVerification.audio = id => player.dispatch({
    type: 'setProp', propName: 'selectedAudioTrackId', propValue: id,
});
window.hlsVerification.destroy = () => player.destroy();
start().catch(error => events.push({ event: 'setupError', error: String(error) }));
