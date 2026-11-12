import { build } from 'esbuild';
import { mkdir } from 'node:fs/promises';

await mkdir('../../target/web-hls-browser', { recursive: true });
await build({
    entryPoints: ['player.js'],
    bundle: true,
    platform: 'browser',
    outfile: '../../target/web-hls-browser/player.js',
    define: { global: 'globalThis', 'process.env.NODE_ENV': '"production"' },
});
