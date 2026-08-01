# AirFlux

Ephemeral peer-to-peer file and text sharing over WebRTC. No servers, no storage, complete privacy.

## How It Works

1. Open the page on Device A — you get a 4-digit room code
2. Share the code (or scan the QR) from Device B
3. A direct WebRTC data channel is established between browsers
4. Send files of any size or text/URLs — all data flows browser-to-browser

## Features

- **P2P File Transfer** — chunked at 64 KB, streamed over WebRTC data channels
- **P2P Text/URL Bridge** — instant clipboard-style messages
- **QR Code Pairing** — scan to connect, no typing needed
- **SHA-256 Integrity Verification** — every file transfer is verified after completion
- **Save to Disk** — uses the File System Access API to stream large files directly to disk (Chromium)
- **Auto-Join via URL** — share a `#join=XXXX` link for instant connection
- **No Servers** — signaling uses PeerJS public server only for initial handshake; all data is direct P2P

## Project Structure

```
index.html   — HTML structure
styles.css   — Custom CSS (zero runtime dependencies)
app.js       — Application logic & WebRTC engine
```

## Running Locally

```bash
# Option 1: any static file server
npx serve .

# Option 2: live reload for development
npx live-server --port=3000
```

## Security

- Links are validated to only allow `http://` and `https://` protocols (XSS prevention)
- All received content is HTML-escaped before rendering
- Clipboard operations include error handling for permission denial
- SHA-256 hashes verify file integrity after transfer

## License

MIT
