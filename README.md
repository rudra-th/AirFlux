# AirFlux

**Browser-to-browser file and text transfer. No accounts, no uploads, no servers in the middle.**

🔗 **Live:** [airflux.netlify.app](https://airflux.netlify.app)

---

## What it does

AirFlux creates a direct WebRTC data channel between two browsers. You get a 4-digit code — the other person enters it — and files or text flow straight from one device to the other, without touching any server.

Useful for: sending a file between your phone and laptop, sharing a link without a messaging app, transferring something quickly without cloud storage.

---

## How to use

1. Open [airflux.netlify.app](https://airflux.netlify.app) on both devices
2. Device A shares its 4-digit code (or the other person scans the QR)
3. Device B enters the code and hits Connect
4. Drop files or paste text — transfers start immediately

Or share a direct join link: `https://airflux.netlify.app/#join=XXXX`

---

## Features

- **P2P file transfer** — chunked at 256 KB, streamed over WebRTC data channels with backpressure control
- **Text & URL bridge** — instant clipboard between devices
- **SHA-256 integrity check** — every file is verified after transfer
- **QR code pairing** — scan to open and auto-join, no typing needed
- **Direct-to-disk streaming** — large files (100 MB+) stream directly to disk via the File System Access API, bypassing RAM (Chrome/Edge)
- **Auto-join via URL** — `#join=XXXX` opens and connects in one tap
- **Resume on reconnect** — interrupted transfers pick up where they left off
- **Zero dependencies at runtime** — PeerJS (signaling only), no frameworks, no build step

---

## Privacy

- No accounts, no sign-in
- Files never touch a server — data flows browser-to-browser over WebRTC
- PeerJS public broker is used only for the initial handshake (exchanging connection info). Once connected, it's out of the picture
- No analytics, no tracking, no ads

---

## Running locally

```bash
# Requires only a static file server
npx serve .

# Or with live reload
npx live-server --port=3000
```

No build step, no npm install, no bundler.

---

## Project structure

```
index.html   — markup and layout
styles.css   — all styles, zero runtime CSS dependencies
app.js       — WebRTC engine, file chunking, UI logic
```

---

## Browser support

| Feature | Chrome | Firefox | Safari | Chrome Android |
|---|---|---|---|---|
| WebRTC / P2P | ✅ | ✅ | ✅ | ✅ |
| Direct-to-disk (File System Access API) | ✅ | ❌ | ❌ | ✅ |
| QR scanning (native camera) | ✅ | ✅ | ✅ | ✅ |

Firefox and Safari fall back gracefully — files download to the browser's default location instead of streaming to a custom path.

---

## Known limitations

- Both devices must be reachable via STUN (works on most home/mobile networks; may fail on strict corporate/university NATs with no TURN fallback)
- PeerJS public signaling server has no uptime SLA — connection setup can occasionally fail if it's down
- No TURN relay, so truly symmetric NATs on both ends can't connect

---

## License

MIT — built by [Rudra](https://github.com/rudra-th/)
