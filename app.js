// ============================================
// AirFlux — P2P File & Text Transfer Engine
// Production-ready build
// ============================================

'use strict';

// ─── CONFIG ────────────────────────────────────────────────────────────────
// 256 KB chunks — sweet spot for raw-mode WebRTC. Large enough to amortise
// per-message overhead; small enough to keep backpressure responsive.
const CHUNK_SIZE          = 256 * 1024;
const PEER_PREFIX         = 'passcode-airdrop-v1-';
const MAX_PEER_RETRIES    = 10;
const CONN_TIMEOUT_MS     = 15000;
// 32 reads × 256 KB = 8 MB pre-read pipeline — keeps disk I/O ahead of network.
// Raw mode is fast enough to drain 4 MB in <100 ms on LAN, so we need a deeper
// pipeline to ensure the sender never stalls waiting for a slice().arrayBuffer().
const READ_AHEAD          = 32;
// Backpressure thresholds — tuned for raw serialization's higher throughput.
// With JSON/binary wrapping gone, the channel can drain 8+ MB/s, so we need a
// larger high-water mark to keep the pipeline full without flooding the buffer.
const BACKPRESSURE_HIGH   = 8 * 1024 * 1024;  // pause above 8 MB buffered
const BACKPRESSURE_LOW    = 1 * 1024 * 1024;  // resume when drained to 1 MB
// ACK every 32 chunks = 8 MB unacked window — matches the new pipeline depth
const ACK_INTERVAL        = 32;
// Max feed cards before oldest are removed (prevents unbounded DOM growth)
const FEED_MAX_CARDS      = 100;
// Auto-prompt disk streaming for files above this size (100 MB) to avoid
// loading the entire file into RAM — important on phones with limited memory.
const LARGE_FILE_THRESHOLD = 100 * 1024 * 1024;
// How long file-end will wait for the user to respond to the save-location
// picker before giving up and falling back to in-RAM assembly. Raw-mode
// transfers can finish large files faster than a user can click through a
// native dialog, so this closes that race rather than buffering silently.
const DISK_DECISION_TIMEOUT_MS = 8000;
// Largest file hashable via fallback path (no DigestStream)
const HASH_FALLBACK_MAX   = 256 * 1024 * 1024; // 256 MB
const HASH_SLICE          = 4  * 1024 * 1024;  // 4 MB per slice

// ─── STATE ─────────────────────────────────────────────────────────────────
let peer                  = null;
let conn                  = null;
let my4DigitCode          = '';
let selectedFiles         = [];
let sendingInProgress     = false;   // guard against double-send
let pendingTransfers      = {};      // fileId → { file, totalChunks, lastAckedChunk }
let incomingFiles         = {};      // fileId → transfer state object
let fileDomCache          = {};      // fileId → { bar, text } — validated on use
let feedItemCount         = 0;
let peerRetryCount        = 0;
let connectionTimeout     = null;
let blobUrlRegistry       = [];      // all live blob URLs — revoked on clearFeed

// Cached DOM refs (set after DOMContentLoaded)
let $statusPill, $statusDot, $statusText;
let $myRoomCode, $joinCodeInput, $feedContainer, $emptyState;
let $feedCount, $dropzone, $dropzonePrompt, $selectedFileState, $retryOverlay;

// Reusable escapeHtml element — created once, never appended to DOM
const _escEl = document.createElement('span');

// ─── AudioContext singleton ─────────────────────────────────────────────────
let _audioCtx = null;
function getAudioCtx() {
    if (!_audioCtx) _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    if (_audioCtx.state === 'suspended') _audioCtx.resume().catch(() => {});
    return _audioCtx;
}

// ─── INIT ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    $statusPill      = id('statusPill');
    $statusDot       = id('statusDot');
    $statusText      = id('statusText');
    $myRoomCode      = id('myRoomCode');
    $joinCodeInput   = id('joinCodeInput');
    $feedContainer   = id('feedContainer');
    $emptyState      = id('emptyState');
    $feedCount       = id('feedCount');
    $dropzone        = id('dropzone');
    $dropzonePrompt  = id('dropzonePrompt');
    $selectedFileState = id('selectedFileState');
    $retryOverlay    = id('retryOverlay');

    initPeer();
    setupDragAndDrop();
    setupPasteToSend();
    checkUrlHashForAutoJoin();

    // Close QR modal on backdrop click
    id('qrModal').addEventListener('click', (e) => {
        if (e.target === id('qrModal')) closeQrModal();
    });

    // Close QR modal on Escape
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeQrModal();
    });
});

// ─── DOM HELPER ─────────────────────────────────────────────────────────────
function id(str) { return document.getElementById(str); }

// ─── PASTE-TO-SEND ──────────────────────────────────────────────────────────
function setupPasteToSend() {
    document.addEventListener('paste', (e) => {
        const active = document.activeElement;
        if (active && (active.id === 'joinCodeInput' || active.id === 'textInput')) return;
        const text = e.clipboardData && e.clipboardData.getData('text');
        if (!text) return;
        e.preventDefault();
        const el = id('textInput');
        if (!el) return;
        el.value = text;
        el.focus();
        showToast('Pasted — press Enter or click Send.', 'info');
    });

    const textEl = id('textInput');
    if (textEl) textEl.addEventListener('keydown', handleTextKeydown);
}

// ─── RAW-MODE FRAMING ────────────────────────────────────────────────────────
// With serialization:'raw', PeerJS sends ArrayBuffers natively with zero wrapping.
// We need to distinguish control messages (JSON) from file chunks (binary).
// Protocol: every message is an ArrayBuffer.
//   - If byte[0] === 0x01 → control message: remaining bytes are UTF-8 JSON
//   - If byte[0] === 0x02 → file chunk:
//       bytes[1..4]   = chunkIndex (Uint32, big-endian)
//       bytes[5..41]  = fileId (36-byte UTF-8 string, zero-padded)
//       bytes[42..]   = chunk payload (raw ArrayBuffer slice)
//
// This avoids all JSON serialization overhead on the hot path (chunks).

const CTRL_TAG  = 0x01;
const CHUNK_TAG = 0x02;
const FILEID_LEN = 36; // max fileId length, zero-padded
const CHUNK_HEADER_SIZE = 1 + 4 + FILEID_LEN; // tag + chunkIndex + fileId

function sendCtrl(obj) {
    const json   = JSON.stringify(obj);
    const enc    = new TextEncoder().encode(json);
    const buf    = new Uint8Array(1 + enc.byteLength);
    buf[0]       = CTRL_TAG;
    buf.set(enc, 1);
    conn.send(buf.buffer);
}

function sendChunk(fileId, chunkIndex, data) {
    const idEnc  = new TextEncoder().encode(fileId.padEnd(FILEID_LEN, '\0'));
    const buf    = new Uint8Array(CHUNK_HEADER_SIZE + data.byteLength);
    const view   = new DataView(buf.buffer);
    buf[0]       = CHUNK_TAG;
    view.setUint32(1, chunkIndex, false); // big-endian
    buf.set(idEnc, 5);
    buf.set(new Uint8Array(data), CHUNK_HEADER_SIZE);
    conn.send(buf.buffer);
}

function decodeRawMessage(rawData) {
    // rawData is ArrayBuffer
    const u8  = new Uint8Array(rawData);
    const tag = u8[0];

    if (tag === CTRL_TAG) {
        try {
            return JSON.parse(new TextDecoder().decode(u8.slice(1)));
        } catch (_) { return null; }
    }

    if (tag === CHUNK_TAG) {
        const view       = new DataView(rawData);
        const chunkIndex = view.getUint32(1, false);
        const fileIdRaw  = new TextDecoder().decode(u8.slice(5, 5 + FILEID_LEN));
        const fileId     = fileIdRaw.replace(/\0+$/, ''); // strip padding
        const data       = rawData.slice(CHUNK_HEADER_SIZE);
        return { type: 'file-chunk', fileId, chunkIndex, data };
    }

    return null;
}

// ─── PEER MANAGEMENT ────────────────────────────────────────────────────────
function generate4DigitCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function initPeer(customCode = null) {
    updateStatus('connecting', 'Signaling…');
    my4DigitCode = customCode || generate4DigitCode();

    peer = new Peer(PEER_PREFIX + my4DigitCode, {
        debug: 0,
        config: {
            iceServers: [
                { urls: 'stun:stun.l.google.com:19302' }
            ]
        }
    });

    peer.on('open', () => {
        $myRoomCode.textContent = my4DigitCode;
        updateStatus('disconnected', 'Ready for peer');
        generateQRCode();
    });

    peer.on('connection', (incoming) => {
        if (conn && conn.open) { incoming.close(); return; }
        // Note: the initiator sets serialization:'raw' — the receiver inherits it
        // automatically from the data channel negotiation. No need to set it here,
        // but we document it explicitly so future readers aren't confused.
        setupConnection(incoming);
    });

    peer.on('error', (err) => {
        console.error('[PeerJS]', err.type, err);
        if (err.type === 'unavailable-id') {
            if (peerRetryCount >= MAX_PEER_RETRIES) {
                showToast('Too many code collisions. Please refresh.', 'error');
                updateStatus('disconnected', 'Ready for peer');
                peerRetryCount = 0;
                return;
            }
            peerRetryCount++;
            const delay = Math.min(1000 * 2 ** (peerRetryCount - 1), 10000);
            showToast(`Code collision — retrying in ${delay / 1000}s…`, 'warning');
            setTimeout(() => initPeer(), delay);
        } else if (err.type === 'peer-unavailable') {
            // If the signaling handshake timed out it's more likely a NAT/network
            // block than a wrong code — give the user a more actionable message.
            const timedOut = !connectionTimeout; // timeout already fired & cleared
            const msg = timedOut
                ? 'Could not reach peer — both devices may be on restrictive networks (e.g. school/office Wi-Fi). Try a mobile hotspot.'
                : 'Peer not found — double-check the code and try again.';
            showToast(msg, 'error');
            updateStatus('disconnected', 'Ready for peer');
            clearConnTimeout();
            showRetryOverlay();
        } else if (err.type === 'network' || err.type === 'server-error') {
            showToast('Signaling server error — retrying…', 'error');
            setTimeout(() => initPeer(), 3000);
        }
    });

    peer.on('disconnected', () => {
        updateStatus('disconnected', 'Disconnected');
        peer.reconnect();
    });
}

function handleJoin(e) {
    if (e && e.preventDefault) e.preventDefault();
    const code = $joinCodeInput.value.trim();
    if (!/^\d{4}$/.test(code)) { showToast('Enter a valid 4-digit code.', 'error'); return; }
    if (code === my4DigitCode) { showToast('Cannot connect to your own code.', 'error'); return; }

    updateStatus('connecting', 'Connecting…');
    hideRetryOverlay();
    // 'raw' skips PeerJS's JSON/binary envelope entirely — ArrayBuffers are sent
    // as-is over the RTCDataChannel. This alone gives 3-5x throughput improvement
    // vs the default 'binary' mode which base64-encodes every chunk.
    const out = peer.connect(PEER_PREFIX + code, { reliable: true, serialization: 'raw' });
    setupConnection(out);

    connectionTimeout = setTimeout(() => {
        if (conn && !conn.open) {
            showToast('Connection timed out.', 'error');
            updateStatus('disconnected', 'Ready for peer');
            conn.close(); conn = null;
            showRetryOverlay();
        }
    }, CONN_TIMEOUT_MS);
}

function setupConnection(connection) {
    conn = connection;

    conn.on('open', () => {
        clearConnTimeout();
        peerRetryCount = 0;
        hideRetryOverlay();
        const code = conn.peer.replace(PEER_PREFIX, '');
        updateStatus('connected', `Connected · ${code}`);
        showToast(`Direct P2P link established with ${code}`, 'success');
        $joinCodeInput.value = '';
        // Visual accent on the code card to signal active connection
        document.querySelector('.my-code-card')?.classList.add('is-connected');

        const ids = Object.keys(pendingTransfers);
        if (ids.length) resumePendingTransfers(ids);
    });

    conn.on('data', handleIncomingData);

    conn.on('close', () => {
        updateStatus('disconnected', 'Not connected');
        showToast('Peer disconnected.', 'warning');
        conn = null;
        document.querySelector('.my-code-card')?.classList.remove('is-connected');
        showRetryOverlay();
    });

    conn.on('error', (err) => {
        console.error('[conn]', err);
        showToast('Data channel error.', 'error');
    });
}

function handleRetry() {
    hideRetryOverlay();
    const code = $joinCodeInput.value.trim();
    if (/^\d{4}$/.test(code) && code !== my4DigitCode) handleJoin(null);
    else $joinCodeInput.focus();
}

async function resumePendingTransfers(ids) {
    for (const fileId of ids) {
        const p = pendingTransfers[fileId];
        if (p && p.file) {
            showToast(`Resuming: ${p.file.name}`, 'info');
            await sendFileOverWebRTC(p.file, p.lastAckedChunk + 1, fileId);
        }
    }
}

function showRetryOverlay()  { $retryOverlay && $retryOverlay.classList.add('visible'); }
function hideRetryOverlay()  { $retryOverlay && $retryOverlay.classList.remove('visible'); }
function clearConnTimeout()  { if (connectionTimeout) { clearTimeout(connectionTimeout); connectionTimeout = null; } }

function updateStatus(state, text) {
    $statusText.textContent = text;
    $statusPill.className   = 'status-pill ' + state;
    $statusDot.className    = 'status-dot '  + state;
}

// ─── FILE-END: ASSEMBLE OR FINALIZE DISK STREAM ─────────────────────────────
// Handles the moment a transfer's last chunk arrives. For files that
// auto-triggered the save-location picker, this waits briefly for the user's
// decision instead of racing ahead and silently buffering the whole file in
// RAM — raw-mode transfers can now outrun a user clicking through a native
// dialog, so this closes that gap rather than defeating the disk-stream
// feature it was built to support.
async function handleFileEnd(fileId, fo) {
    // If the user hasn't answered the save picker yet but might still (i.e.
    // it auto-opened for this file and hasn't resolved), give it a bounded
    // window rather than assembling into RAM immediately.
    if (fo.isLargeAutoDisk && !fo.diskWritable && fo.diskDecisionPromise) {
        showToast(`Finishing ${fo.name} — waiting for you to choose a save location…`, 'info');
        await Promise.race([
            fo.diskDecisionPromise,
            new Promise(resolve => setTimeout(() => resolve('timeout'), DISK_DECISION_TIMEOUT_MS))
        ]);
        // fo is the same object reference — diskWritable will now be set if
        // the user picked a location in time, whether via the timeout race
        // above or the promise resolving first.
    }

    if (fo.diskWritable) {
        fo.pendingDiskWrite.then(async () => {
            try { await fo.diskWritable.close(); } catch (_) {}
            showDiskSaveComplete(fileId);
        });
        finishFileCard(fileId, null, fo.name, fo.size, 'pending');
    } else {
        // Either a normal file, or a large file where the user never
        // responded to the picker in time — falls back to in-RAM assembly.
        if (fo.isLargeAutoDisk) {
            showToast(`${fo.name} didn't get a save location in time — kept in memory instead.`, 'warning');
        }
        // Assemble in index order — chunks may have arrived out of order
        const sorted = Array.from(fo.buffers.entries())
            .sort((a, b) => a[0] - b[0])
            .map(([, buf]) => buf)
            .filter(Boolean);
        const blob = new Blob(sorted, { type: fo.fileType });
        const url  = URL.createObjectURL(blob);
        fo.assembledBlob = blob;
        fo.downloadUrl   = url;
        blobUrlRegistry.push(url);
        finishFileCard(fileId, url, fo.name, fo.size, 'pending');
    }
    showToast(`Received: ${fo.name} — verifying…`, 'info');
    playReceiveSound();
    sendCtrl({ type: 'file-ack', fileId, lastChunkIndex: fo.totalChunks - 1 });
}

// ─── FILE-HASH: VERIFY AFTER FINALIZATION ───────────────────────────────────
// Waits for handleFileEnd() to finish (including any disk-decision wait)
// before checking assembledBlob — otherwise a still-finalizing large file
// would look "not ready" here and get its state deleted out from under it.
//
// Note: this no longer deletes incomingFiles[fileId] on completion. Doing so
// used to make the post-completion "Save to…" button silently non-functional,
// since it reads fo.assembledBlob from that same map. The entry is now kept
// until the card is evicted from the feed or the feed is cleared (both of
// which properly clean up incomingFiles alongside the DOM/blob cleanup).
async function finalizeAndVerify(data, fo) {
    if (fo.finalizePromise) await fo.finalizePromise;

    if (fo.diskWritable) {
        // Cannot re-read from disk — trust the transfer
        finishFileCard(data.fileId, null, fo.name, fo.size, 'verified');
        showToast(`Saved & verified: ${fo.name}`, 'success');
        fo.completed = true;
        return;
    }

    const blob = fo.assembledBlob;
    if (!blob) { fo.completed = true; return; }

    const localHash = await computeFileHash(blob);
    let status;
    if (localHash === null || data.hash === null) status = 'unverified';
    else if (localHash === data.hash)             status = 'verified';
    else                                           status = 'mismatch';

    finishFileCard(data.fileId, fo.downloadUrl, fo.name, fo.size, status);
    const msgs = {
        verified:   `Integrity verified: ${fo.name}`,
        mismatch:   `Hash mismatch — ${fo.name} may be corrupted`,
        unverified: `${fo.name} received (file too large for checksum)`
    };
    showToast(msgs[status], status === 'verified' ? 'success' : status === 'mismatch' ? 'warning' : 'info');
    fo.completed = true;
}

// ─── INCOMING DATA HANDLER ───────────────────────────────────────────────────
function handleIncomingData(rawData) {
    // In raw serialization mode every message arrives as an ArrayBuffer.
    // Decode it using our framing protocol before dispatching.
    const data = (rawData instanceof ArrayBuffer) ? decodeRawMessage(rawData) : rawData;
    if (!data) return;

    switch (data.type) {

    case 'text':
        addIncomingTextCard(data.content, data.timestamp);
        autoSwitchToReceive();
        playNotificationPulse();
        playReceiveSound();
        break;

    case 'file-start':
    case 'file-resume': {
        const isResume = data.type === 'file-resume';
        // On resume, preserve existing progress if we already have this transfer
        if (isResume && incomingFiles[data.fileId]) {
            incomingFiles[data.fileId].totalChunks = data.totalChunks;
            const fo = incomingFiles[data.fileId];
            updateFileCardProgress(data.fileId,
                pct(fo.receivedChunks, fo.totalChunks), 0, 0);
            showToast(`Resuming: ${data.name} from ${pct(fo.receivedChunks, fo.totalChunks)}%`, 'info');
            playNotificationPulse();
            break;
        }
        incomingFiles[data.fileId] = {
            name: data.name,
            size: data.size,
            fileType: data.fileType || 'application/octet-stream',
            totalChunks: data.totalChunks,
            receivedChunks: 0,
            receivedBytes: 0,
            // Single Map — dedup key + storage. Nulled on disk-stream activation;
            // a separate diskSeenChunks Set takes over dedup from that point.
            buffers: new Map(),
            diskSeenChunks: null,   // Set<number> activated when buffers nulled
            startTime: performance.now(),
            lastSpeedUpdate: performance.now(),
            lastBytesUpdate: 0,
            lastDomUpdate: 0,
            diskWritable: null,
            diskReady: false,
            pendingDiskChunks: [],
            pendingDiskWrite: Promise.resolve(),
            assembledBlob: null,
            downloadUrl: null,
            // Large-file race guard: if this file auto-triggers the save picker,
            // file-end will wait briefly for the user's decision before falling
            // back to full in-RAM assembly. See handleFileEnd().
            isLargeAutoDisk: false,
            diskDecisionResolve: null,
            diskDecisionPromise: null
        };
        addIncomingFileCard(data.fileId, data.name, data.size);
        autoSwitchToReceive();
        playNotificationPulse();
        if (isResume) showToast(`Receiving: ${data.name}`, 'info');

        // For large files, automatically start disk streaming so the entire
        // file isn't held in RAM (critical on phones). The picker opens now
        // while chunks stream in the background; already-received chunks are
        // drained to disk once the user picks a location.
        //
        // Raw-mode transfers can now finish a 100 MB file in seconds — faster
        // than a user can realistically respond to the native save dialog. So
        // we track the decision with a resolvable promise: handleFileEnd()
        // waits up to DISK_DECISION_TIMEOUT_MS for the user to answer before
        // falling back to in-RAM assembly, instead of racing ahead blindly.
        if (data.size >= LARGE_FILE_THRESHOLD && typeof window.showSaveFilePicker === 'function') {
            const fo = incomingFiles[data.fileId];
            fo.isLargeAutoDisk = true;
            fo.diskDecisionPromise = new Promise(resolve => { fo.diskDecisionResolve = resolve; });
            showToast(`Large file (${fmtBytes(data.size)}) — choose where to save it now to stream directly to disk.`, 'info');
            // Small delay so the card renders first, then open picker
            setTimeout(() => startStreamToDisk(data.fileId), 300);
        }
        break;
    }

    case 'file-chunk': {
        const fo = incomingFiles[data.fileId];
        if (!fo) return;

        // Dedup — use Map when available, diskSeenChunks Set after disk activation
        if (fo.buffers) {
            if (fo.buffers.has(data.chunkIndex)) return;
        } else {
            if (!fo.diskSeenChunks) fo.diskSeenChunks = new Set();
            if (fo.diskSeenChunks.has(data.chunkIndex)) return;
            fo.diskSeenChunks.add(data.chunkIndex);
        }

        if (fo.diskReady && fo.diskWritable) {
            // Live disk stream — chain writes to preserve order
            const chunkData = data.data;
            fo.pendingDiskWrite = fo.pendingDiskWrite
                .then(() => fo.diskWritable.write(new Uint8Array(chunkData)))
                .catch(() => {});
        } else if (fo.diskWritable && !fo.diskReady) {
            // Picker open but drain not started — queue
            fo.pendingDiskChunks.push({ index: data.chunkIndex, data: data.data });
            if (fo.buffers) fo.buffers.set(data.chunkIndex, null); // dedup marker
        } else {
            fo.buffers.set(data.chunkIndex, data.data);
        }

        fo.receivedChunks++;
        fo.receivedBytes += data.data.byteLength;

        if (fo.receivedChunks % ACK_INTERVAL === 0) {
            sendCtrl({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: data.chunkIndex });
        }

        // Throttle DOM to ≤12fps
        const now = performance.now();
        if (now - fo.lastDomUpdate < 80) return;
        fo.lastDomUpdate = now;

        let speed = 0;
        const warmup = now - fo.startTime;
        // Don't show speed until 500 ms in — avoids inflated spike from first burst.
        if (warmup >= 500) {
            const elapsed = now - fo.lastSpeedUpdate;
            if (elapsed > 500) {
                speed = (fo.receivedBytes - fo.lastBytesUpdate) / (elapsed / 1000);
                fo.lastSpeedUpdate = now;
                fo.lastBytesUpdate = fo.receivedBytes;
            } else {
                speed = fo.receivedBytes / (warmup / 1000);
            }
        }
        const remaining = speed > 0 ? (fo.size - fo.receivedBytes) / speed : 0;
        updateFileCardProgress(data.fileId, pct(fo.receivedChunks, fo.totalChunks), speed, remaining);
        break;
    }

    case 'file-end': {
        const fo = incomingFiles[data.fileId];
        if (!fo) return;
        // Store the promise so file-hash (which can arrive moments later) can
        // wait for it — otherwise a large auto-disk file still waiting on its
        // save-location decision would look "not assembled yet" to file-hash
        // and get deleted prematurely. See handleFileEnd() for the race this
        // guards against.
        fo.finalizePromise = handleFileEnd(data.fileId, fo);
        break;
    }

    case 'file-hash': {
        const fo = incomingFiles[data.fileId];
        if (!fo) return;
        finalizeAndVerify(data, fo);
        break;
    }

    case 'file-ack': {
        const p = pendingTransfers[data.fileId];
        if (p) p.lastAckedChunk = Math.max(p.lastAckedChunk, data.lastChunkIndex);
        break;
    }

    } // end switch
}

// ─── FILE SENDING ────────────────────────────────────────────────────────────
async function sendFileOverWebRTC(file, resumeFrom = 0, existingFileId = null) {
    if (!conn || !conn.open) {
        showToast('No active connection — connect first.', 'error');
        return;
    }

    const fileId      = existingFileId || Math.random().toString(36).slice(2, 11);
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE) || 1;

    sendCtrl({
        type: resumeFrom > 0 ? 'file-resume' : 'file-start',
        fileId, name: file.name, size: file.size,
        fileType: file.type || 'application/octet-stream',
        totalChunks, resumeFrom
    });

    pendingTransfers[fileId] = { file, totalChunks, lastAckedChunk: resumeFrom - 1 };

    if (id(`file-card-${fileId}`)) {
        updateFileCardProgress(fileId, pct(resumeFrom, totalChunks), 0, 0);
    } else {
        addOutgoingFileCard(fileId, file.name, file.size);
    }

    // Event-driven backpressure — safely access the underlying RTCDataChannel.
    // PeerJS doesn't guarantee a stable .dataChannel property, so we try several paths.
    const dc = (() => {
        try { return conn.dataChannel || conn._dc || null; }
        catch (_) { return null; }
    })();

    let drainResolve = null;
    let drainListener = null;
    if (dc) {
        dc.bufferedAmountLowThreshold = BACKPRESSURE_LOW;
        drainListener = () => {
            if (drainResolve) { const r = drainResolve; drainResolve = null; r(); }
        };
        dc.addEventListener('bufferedamountlow', drainListener);
    }

    // Level-triggered: if the buffer is already below the low-water mark when we
    // check (e.g. the 'bufferedamountlow' event already fired before we awaited),
    // resolve immediately instead of deadlocking indefinitely.
    const waitForDrain = () => {
        if (!dc || dc.bufferedAmount <= BACKPRESSURE_LOW) return Promise.resolve();
        return new Promise(r => { drainResolve = r; });
    };

    // Pipelined read-ahead — disk reads overlap with network sends
    const readQueue = [];
    let readOffset  = resumeFrom * CHUNK_SIZE;
    function enqueueRead() {
        if (readOffset >= file.size) return;
        const end = Math.min(readOffset + CHUNK_SIZE, file.size);
        readQueue.push(file.slice(readOffset, end).arrayBuffer());
        readOffset = end;
    }
    for (let i = 0; i < READ_AHEAD; i++) enqueueRead();

    const startTime      = performance.now();
    let bytesSent        = resumeFrom * CHUNK_SIZE;
    let lastSpeedSample  = startTime;
    let lastBytesSample  = bytesSent;
    let lastDomUpdate    = 0;

    for (let chunkIndex = resumeFrom; chunkIndex < totalChunks; chunkIndex++) {
        const bufPromise = readQueue.shift();
        if (!bufPromise) break;
        enqueueRead(); // kick off next read before awaiting current

        const buffer = await bufPromise;
        if (!buffer) break;

        if (dc && dc.bufferedAmount > BACKPRESSURE_HIGH) await waitForDrain();

        try {
            // sendChunk sends raw ArrayBuffer with a tiny binary header —
            // no JSON serialization, no base64, no PeerJS envelope overhead.
            sendChunk(fileId, chunkIndex, buffer);
        } catch (_) {
            showToast('Transfer interrupted — will resume on reconnect.', 'error');
            finishOutgoingFileCard(fileId, '?', 0);
            if (dc && drainListener) dc.removeEventListener('bufferedamountlow', drainListener);
            return;
        }

        bytesSent += buffer.byteLength;

        const now = performance.now();
        if (now - lastDomUpdate >= 100) {
            lastDomUpdate = now;
            let speed = 0;
            const wms = now - lastSpeedSample;
            // Only show speed after 500 ms of data — avoids the misleading
            // "50 MB/s" spike from the first burst before the channel settles.
            const warmup = now - startTime;
            if (warmup >= 500) {
                if (wms > 400) {
                    speed = (bytesSent - lastBytesSample) / (wms / 1000);
                    lastSpeedSample = now; lastBytesSample = bytesSent;
                } else {
                    speed = bytesSent / (warmup / 1000);
                }
            }
            updateFileCardProgress(fileId, pct(chunkIndex + 1, totalChunks), speed,
                speed > 0 ? (file.size - bytesSent) / speed : 0);
        }
    }

    // Clean up listener — prevents accumulation over multiple files
    if (dc && drainListener) dc.removeEventListener('bufferedamountlow', drainListener);

    delete pendingTransfers[fileId];
    sendCtrl({ type: 'file-end', fileId });

    // Hash the File directly (File extends Blob — no copy needed)
    computeFileHash(file).then(hash => sendCtrl({ type: 'file-hash', fileId, hash }));

    const elapsed = (performance.now() - startTime) / 1000;
    const avgSpeed = elapsed > 0 ? file.size / elapsed : 0;
    updateFileCardProgress(fileId, 100, avgSpeed, 0);
    finishOutgoingFileCard(fileId, elapsed.toFixed(1), avgSpeed);
    showToast(`Sent: ${file.name} — ${elapsed.toFixed(1)}s · avg ${fmtSpeed(avgSpeed)}`, 'success');
}

// ─── TEXT SEND ───────────────────────────────────────────────────────────────
function sendText() {
    const el      = id('textInput');
    const content = el.value.trim();
    if (!content) return;
    if (!conn || !conn.open) { showToast('Connect to a peer first.', 'error'); return; }

    const ts = Date.now();
    sendCtrl({ type: 'text', content, timestamp: ts });
    addOutgoingTextCard(content, ts);
    el.value = '';
    showToast('Text sent.', 'success');
}

// Enter = send, Shift+Enter = newline
function handleTextKeydown(e) {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendText(); }
}

// ─── DRAG & DROP ─────────────────────────────────────────────────────────────
function setupDragAndDrop() {
    ['dragenter','dragover','dragleave','drop'].forEach(ev =>
        $dropzone.addEventListener(ev, e => { e.preventDefault(); e.stopPropagation(); }));
    ['dragenter','dragover'].forEach(ev =>
        $dropzone.addEventListener(ev, () => $dropzone.classList.add('dragover')));
    ['dragleave','drop'].forEach(ev =>
        $dropzone.addEventListener(ev, () => $dropzone.classList.remove('dragover')));
    $dropzone.addEventListener('drop', e => {
        const files = e.dataTransfer && e.dataTransfer.files;
        if (files && files.length) addFilesToQueue(Array.from(files));
    });
}

function handleFileSelect(e) {
    if (e.target.files && e.target.files.length) {
        addFilesToQueue(Array.from(e.target.files));
        e.target.value = '';
    }
}

function addFilesToQueue(newFiles) {
    for (const f of newFiles) {
        if (!selectedFiles.some(s => s.name === f.name && s.size === f.size))
            selectedFiles.push(f);
    }
    renderFileQueue();
}

function removeFile(index) {
    selectedFiles.splice(index, 1);
    renderFileQueue();
}

function clearAllFiles(e) {
    if (e) e.stopPropagation();
    selectedFiles = [];
    const fi = id('fileInput');
    if (fi) fi.value = '';
    $selectedFileState.classList.add('hidden');
    $dropzonePrompt.classList.remove('hidden');
}

function renderFileQueue() {
    if (!selectedFiles.length) { clearAllFiles(null); return; }
    $dropzonePrompt.classList.add('hidden');
    $selectedFileState.classList.remove('hidden');

    const list    = id('fileList');
    const summary = id('fileListSummary');

    // Build HTML using data-index to avoid the XSS risk of putting content
    // in onclick string attributes. Click is delegated below.
    list.innerHTML = selectedFiles.map((f, i) => `
        <div class="file-list-item">
            <div class="selected-file-info">
                <div class="file-icon green">
                    <i class="fa-solid ${fileIcon(f.name)}"></i>
                </div>
                <div class="file-details">
                    <p class="file-name" title="${esc(f.name)}">${esc(f.name)}</p>
                    <p class="file-size">${fmtBytes(f.size)}</p>
                </div>
            </div>
            <button type="button" class="btn-clear-file" data-remove-index="${i}" title="Remove" aria-label="Remove ${esc(f.name)}">
                <i class="fa-solid fa-xmark" aria-hidden="true"></i>
            </button>
        </div>
    `).join('');

    // Delegate remove clicks — stopPropagation is essential here: without it,
    // the click bubbles to the dropzone's own onclick and reopens the file picker.
    list.onclick = (e) => {
        const btn = e.target.closest('[data-remove-index]');
        if (btn) { e.stopPropagation(); removeFile(Number(btn.dataset.removeIndex)); }
    };

    const total = selectedFiles.reduce((s, f) => s + f.size, 0);
    summary.textContent =
        `${selectedFiles.length} file${selectedFiles.length !== 1 ? 's' : ''} — ${fmtBytes(total)}`;
}

async function sendAllFiles(e) {
    if (e) e.stopPropagation();
    if (!selectedFiles.length) return;
    if (!conn || !conn.open) { showToast('Connect to a peer first.', 'error'); return; }
    if (sendingInProgress)    { showToast('Transfer already in progress.', 'warning'); return; }

    sendingInProgress = true;
    const btn = id('sendFilesBtn');
    if (btn) { btn.disabled = true; btn.textContent = 'Sending…'; }

    const files = [...selectedFiles];
    clearAllFiles(null);
    try {
        for (const file of files) await sendFileOverWebRTC(file);
    } finally {
        sendingInProgress = false;
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-paper-plane" aria-hidden="true"></i> Send to Peer'; }
    }
}

// ─── DISK STREAMING ──────────────────────────────────────────────────────────
async function startStreamToDisk(fileId) {
    const fo = incomingFiles[fileId];
    if (!fo || fo.diskWritable) return;

    try {
        const handle   = await window.showSaveFilePicker({
            suggestedName: fo.name,
            types: [{ description: 'All Files', accept: { '*/*': [] } }]
        });
        const writable = await handle.createWritable();
        fo.diskHandle   = handle;
        fo.diskWritable = writable;

        const btn = id(`save-disk-btn-${fileId}`);
        if (btn) {
            btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin" aria-hidden="true"></i> Saving…';
            btn.disabled  = true;
            btn.onclick   = null;
        }

        // Drain already-buffered chunks in order, then also queued ones
        const already = Array.from((fo.buffers || new Map()).entries())
            .filter(([, buf]) => buf != null)
            .map(([idx, buf]) => ({ index: idx, data: buf }));
        const queued  = fo.pendingDiskChunks.splice(0);
        const all     = [...already, ...queued].sort((a, b) => a.index - b.index);

        // Null out the Map — activate diskSeenChunks Set for future dedup
        fo.buffers          = null;
        fo.diskSeenChunks   = new Set(all.map(c => c.index));

        fo.pendingDiskWrite = fo.pendingDiskWrite.then(async () => {
            for (const chunk of all)
                await writable.write(new Uint8Array(chunk.data));
        });

        fo.diskReady = true;
        showToast('Streaming to disk…', 'info');
        // Unblock handleFileEnd() if it's waiting on this decision
        if (fo.diskDecisionResolve) { fo.diskDecisionResolve('disk'); fo.diskDecisionResolve = null; }
    } catch (err) {
        if (err.name !== 'AbortError') showToast('Could not open save dialog.', 'error');
        // User cancelled or the picker failed — resolve immediately so
        // handleFileEnd() doesn't wait out the full timeout for nothing.
        if (fo.diskDecisionResolve) { fo.diskDecisionResolve('cancelled'); fo.diskDecisionResolve = null; }
    }
}

function showDiskSaveComplete(fileId) {
    const btn = id(`save-disk-btn-${fileId}`);
    if (btn) {
        btn.innerHTML = '<i class="fa-solid fa-check" aria-hidden="true"></i> Saved to Disk';
        btn.disabled  = false;
    }
}

// Post-transfer save via showSaveFilePicker (uses the in-memory blob directly)
async function streamToDisk(fileId) {
    const fo = incomingFiles[fileId];
    if (!fo || !fo.assembledBlob) { showToast('File data no longer available.', 'error'); return; }
    try {
        const handle   = await window.showSaveFilePicker({
            suggestedName: fo.name,
            types: [{ description: 'All Files', accept: { '*/*': [] } }]
        });
        const writable = await handle.createWritable();
        await writable.write(fo.assembledBlob);
        await writable.close();
        showToast(`Saved: ${fo.name}`, 'success');
    } catch (err) {
        if (err.name !== 'AbortError') showToast('Save failed.', 'error');
    }
}

// ─── FEED: EMPTY STATE + COUNT ────────────────────────────────────────────────
function syncFeedMeta() {
    $emptyState.classList.toggle('hidden', feedItemCount > 0);
    // Keep both the mobile tab badge and desktop badge in sync
    $feedCount.textContent = feedItemCount;
    const desktop = id('feedCountDesktop');
    if (desktop) desktop.textContent = feedItemCount;
}

// Evict oldest card when feed exceeds max to keep DOM bounded
function evictOldestCard() {
    if (feedItemCount <= FEED_MAX_CARDS) return;
    // Cards are prepended so the oldest is the last child (before emptyState)
    const children = Array.from($feedContainer.children).filter(c => c !== $emptyState);
    if (children.length > FEED_MAX_CARDS) {
        const oldest = children[children.length - 1];
        // Revoke any blob URL attached to this card
        const dl = oldest.querySelector('a[download]');
        if (dl) {
            const href = dl.getAttribute('href');
            if (href && href.startsWith('blob:')) {
                URL.revokeObjectURL(href);
                blobUrlRegistry = blobUrlRegistry.filter(u => u !== href);
            }
        }
        // File cards keep their incomingFiles[fileId] entry alive after
        // completion now (so the post-completion Save button still works) —
        // clean it up here instead, once the card actually leaves the feed.
        if (oldest.id && oldest.id.startsWith('file-card-')) {
            delete incomingFiles[oldest.id.slice('file-card-'.length)];
        }
        $feedContainer.removeChild(oldest);
        feedItemCount--;
    }
}

// ─── FEED CARDS ──────────────────────────────────────────────────────────────
function addIncomingTextCard(content, timestamp) {
    feedItemCount++;
    evictOldestCard();
    syncFeedMeta();

    const isUrl = validUrl(content);
    const card  = document.createElement('div');
    card.className = 'feed-card feed-in';

    // Use data attributes for copy/open to avoid inline onclick XSS risk
    card.innerHTML = `
        <div class="card-header">
            <span class="badge ${isUrl ? 'badge-violet' : 'badge-violet'}">
                <i class="fa-solid ${isUrl ? 'fa-link' : 'fa-message'}" aria-hidden="true"></i>
                ${isUrl ? 'URL' : 'Text'}
            </span>
            <span class="badge-time">${fmtTime(timestamp)}</span>
        </div>
        <div class="card-body">${esc(content)}</div>
        <div class="card-actions">
            ${isUrl ? `<button class="btn-action btn-action-primary" data-open-url>
                <i class="fa-solid fa-arrow-up-right-from-square" aria-hidden="true"></i> Open
            </button>` : ''}
            <button class="btn-action btn-action-ghost" data-copy-text>
                <i class="fa-regular fa-copy" aria-hidden="true"></i> Copy
            </button>
        </div>`;

    // Delegate events — no inline onclick, content stays in data only
    card.querySelector('[data-copy-text]')?.addEventListener('click', function() {
        copyText(content, this);
    });
    if (isUrl) {
        card.querySelector('[data-open-url]')?.addEventListener('click', () => {
            window.open(content, '_blank', 'noopener,noreferrer');
        });
    }

    $feedContainer.insertBefore(card, $feedContainer.firstChild);
}

function addOutgoingTextCard(content, timestamp) {
    feedItemCount++;
    evictOldestCard();
    syncFeedMeta();

    const card = document.createElement('div');
    card.className = 'feed-card outgoing-text';
    card.innerHTML = `
        <div class="card-header">
            <span class="badge badge-dim">
                <i class="fa-solid fa-arrow-right-from-bracket" aria-hidden="true"></i> Sent
            </span>
            <span class="badge-time">${fmtTime(timestamp)}</span>
        </div>
        <div class="card-body dim">${esc(content)}</div>`;
    $feedContainer.insertBefore(card, $feedContainer.firstChild);
}

function addIncomingFileCard(fileId, name, size) {
    feedItemCount++;
    evictOldestCard();
    syncFeedMeta();

    const hasDiskApi = typeof window.showSaveFilePicker === 'function';
    const card = document.createElement('div');
    card.id        = `file-card-${fileId}`;
    card.className = 'file-card';
    card.innerHTML = `
        <div class="card-header">
            <span class="badge badge-green">
                <i class="fa-solid fa-cloud-arrow-down" aria-hidden="true"></i> Incoming
            </span>
            <span class="file-size-tag">${fmtBytes(size)}</span>
        </div>
        <div class="file-card-row">
            <div class="file-icon green" aria-hidden="true">
                <i class="fa-solid ${fileIcon(name)}"></i>
            </div>
            <div class="file-card-meta">
                <p class="file-card-name" title="${esc(name)}">${esc(name)}</p>
                <p class="file-card-progress">Receiving… 0%</p>
            </div>
        </div>
        <div class="prog-track" role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">
            <div class="prog-bar green" style="width:0%"></div>
        </div>
        <div class="file-preview" id="preview-${fileId}"></div>
        <div class="file-card-actions ${hasDiskApi ? 'show' : ''}">
            ${hasDiskApi ? `<button class="btn-action btn-action-ghost" id="save-disk-btn-${fileId}" data-disk-stream="${fileId}">
                <i class="fa-solid fa-hard-drive" aria-hidden="true"></i> Save to Disk
            </button>` : ''}
        </div>`;

    card.querySelector(`[data-disk-stream]`)?.addEventListener('click', () =>
        startStreamToDisk(fileId));

    $feedContainer.insertBefore(card, $feedContainer.firstChild);
}

function addOutgoingFileCard(fileId, name, size) {
    feedItemCount++;
    evictOldestCard();
    syncFeedMeta();

    const card = document.createElement('div');
    card.id        = `file-card-${fileId}`;
    card.className = 'file-card outgoing';
    card.innerHTML = `
        <div class="card-header">
            <span class="badge badge-sky">
                <i class="fa-solid fa-cloud-arrow-up" aria-hidden="true"></i> Sending
            </span>
            <span class="file-size-tag">${fmtBytes(size)}</span>
        </div>
        <div class="file-card-row">
            <div class="file-icon sky" aria-hidden="true">
                <i class="fa-solid ${fileIcon(name)}"></i>
            </div>
            <div class="file-card-meta">
                <p class="file-card-name">${esc(name)}</p>
                <p class="file-card-progress">Streaming… 0%</p>
            </div>
        </div>
        <div class="prog-track" role="progressbar" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">
            <div class="prog-bar sky" style="width:0%"></div>
        </div>`;
    $feedContainer.insertBefore(card, $feedContainer.firstChild);
}

// Guard cached DOM refs against stale detached nodes
function updateFileCardProgress(fileId, percent, speed, remaining) {
    let c = fileDomCache[fileId];
    if (c && (!c.bar.isConnected || !c.text.isConnected)) {
        delete fileDomCache[fileId]; c = null;
    }
    if (!c) {
        const card = id(`file-card-${fileId}`);
        if (!card) return;
        const bar  = card.querySelector('.prog-bar');
        const text = card.querySelector('.file-card-progress');
        const track = card.querySelector('.prog-track');
        if (!bar || !text) return;
        fileDomCache[fileId] = c = { bar, text, track };
    }
    c.bar.style.width = `${percent}%`;
    if (c.track) { c.track.setAttribute('aria-valuenow', percent); }
    const sp  = speed > 0 ? ` · ${fmtSpeed(speed)}` : '';
    const eta = remaining > 1 ? ` · ${fmtETA(remaining)}` : '';
    c.text.textContent = `${percent}%${sp}${eta}`;
}

function finishFileCard(fileId, downloadUrl, name, size, status) {
    delete fileDomCache[fileId];
    const card = id(`file-card-${fileId}`);
    if (!card) return;

    const fo         = incomingFiles[fileId];
    const wasStreamed = fo && fo.diskWritable;

    const textEl = card.querySelector('.file-card-progress');
    if (textEl) {
        const msg = {
            pending:    'Complete — verifying integrity…',
            verified:   'Complete — integrity verified ✓',
            mismatch:   'Complete — hash MISMATCH (may be corrupted)',
            unverified: 'Complete — no checksum (file >256 MB)'
        };
        textEl.textContent = msg[status] || 'Complete';
        if (status === 'mismatch') textEl.classList.add('error');
    }

    const bar   = card.querySelector('.prog-bar');
    const track = card.querySelector('.prog-track');
    if (bar)   { bar.style.width = '100%'; bar.classList.replace('green','green-done'); }
    if (track) { track.setAttribute('aria-valuenow', 100); }

    const preview = card.querySelector('.file-preview');
    if (preview && !preview.hasChildNodes() && downloadUrl && fo && fo.fileType)
        addFilePreview(preview, fo.fileType, downloadUrl);

    const area = card.querySelector('.file-card-actions');
    if (!area) return;
    area.classList.add('show');

    const vb = {
        pending:    ['vbadge-amber',   'fa-spinner fa-spin',       'Verifying…'],
        verified:   ['vbadge-green', 'fa-shield-check',          'Verified'],
        mismatch:   ['vbadge-red',     'fa-triangle-exclamation',  'Hash mismatch'],
        unverified: ['vbadge-dim',   'fa-circle-info',           'No checksum'],
    }[status] || ['vbadge-dim', 'fa-hard-drive', fmtBytes(size)];

    if (wasStreamed) {
        // Already saved to disk — just show the verification badge
        area.innerHTML = `
            <span class="vbadge ${vb[0]}">
                <i class="fa-solid ${vb[1]}" aria-hidden="true"></i> ${vb[2]}
            </span>
            <span class="vbadge vbadge-dim">
                <i class="fa-solid fa-hard-drive" aria-hidden="true"></i> Saved to disk
            </span>`;
    } else {
        // Two clean actions: Download (primary) + Save to Custom Location (secondary)
        const hasDiskApi = typeof window.showSaveFilePicker === 'function';
        area.innerHTML = `
            <span class="vbadge ${vb[0]}">
                <i class="fa-solid ${vb[1]}" aria-hidden="true"></i> ${vb[2]}
            </span>
            <a href="${downloadUrl}" download="${esc(name)}" class="btn-action btn-action-download">
                <i class="fa-solid fa-download" aria-hidden="true"></i> Download
            </a>
            ${hasDiskApi ? `<button class="btn-action btn-action-save" data-disk-save="${fileId}" title="Choose where to save this file">
                <i class="fa-solid fa-folder-open" aria-hidden="true"></i> Save to…
            </button>` : ''}`;

        area.querySelector('[data-disk-save]')?.addEventListener('click', () =>
            streamToDisk(fileId));

        // Revoke blob URL after download starts (10 s grace)
        area.querySelector('a[download]')?.addEventListener('click', () => {
            setTimeout(() => {
                URL.revokeObjectURL(downloadUrl);
                blobUrlRegistry = blobUrlRegistry.filter(u => u !== downloadUrl);
            }, 10000);
        });
    }
}

function finishOutgoingFileCard(fileId, totalTime, avgSpeed) {
    const card = id(`file-card-${fileId}`);
    if (!card) return;
    const text = card.querySelector('.file-card-progress');
    if (text) text.textContent =
        `Delivered in ${totalTime}s${avgSpeed > 0 ? `, avg ${fmtSpeed(avgSpeed)}` : ''}`;
}

// Clears feed, revokes all blob URLs, resets cache
function clearFeed() {
    while ($feedContainer.firstChild) $feedContainer.removeChild($feedContainer.firstChild);
    $feedContainer.appendChild($emptyState);
    feedItemCount = 0;
    fileDomCache  = {};

    // Revoke all tracked blob URLs — releases memory
    for (const url of blobUrlRegistry) URL.revokeObjectURL(url);
    blobUrlRegistry = [];

    // incomingFiles entries now persist after completion (so the post-completion
    // Save button keeps working) — clearing the whole feed is the point where
    // they finally get released, since no card references them anymore.
    incomingFiles = {};

    syncFeedMeta();
    showToast('Feed cleared.', 'info');
}

// ─── FILE PREVIEW ─────────────────────────────────────────────────────────────
function addFilePreview(container, fileType, blobUrl) {
    if (fileType.startsWith('image/')) {
        const img  = document.createElement('img');
        img.src    = blobUrl;
        img.className = 'preview-img';
        img.loading   = 'lazy';
        img.alt       = 'Preview';
        img.onload    = () => container.appendChild(img);
    } else if (fileType.startsWith('video/')) {
        const vid  = document.createElement('video');
        vid.src    = blobUrl;
        vid.className = 'preview-vid';
        vid.controls  = true;
        vid.preload   = 'metadata';
        container.appendChild(vid);
    } else if (fileType.startsWith('audio/')) {
        const aud  = document.createElement('audio');
        aud.src    = blobUrl;
        aud.className = 'preview-aud';
        aud.controls  = true;
        container.appendChild(aud);
    } else if (fileType.startsWith('text/') ||
               fileType === 'application/json' ||
               fileType === 'application/javascript') {
        fetch(blobUrl).then(r => r.text()).then(text => {
            const pre = document.createElement('pre');
            pre.className = 'preview-txt';
            pre.textContent = text.length > 600 ? text.slice(0, 600) + '\n…' : text;
            container.appendChild(pre);
        }).catch(() => {});
    }
}

// ─── QR CODE ─────────────────────────────────────────────────────────────────
function generateQRCode() {
    const box = id('qrcode');
    if (!box) return;
    box.innerHTML = '';
    const url = `${location.origin}${location.pathname}#join=${my4DigitCode}`;
    id('shareUrlText').textContent = url;
    new QRCode(box, { text: url, width: 180, height: 180,
        colorDark: '#020617', colorLight: '#ffffff',
        correctLevel: QRCode.CorrectLevel.M });
}

function openQrModal() {
    // Guard: my4DigitCode is '' until peer opens — not '----'
    if (!my4DigitCode) { showToast('Still connecting — try in a moment.', 'warning'); return; }
    const modal   = id('qrModal');
    const content = id('qrModalContent');
    modal.classList.add('active');
    requestAnimationFrame(() => {
        modal.classList.add('visible');
        content.classList.add('scale-in');
    });
}

function closeQrModal() {
    const modal   = id('qrModal');
    const content = id('qrModalContent');
    modal.classList.remove('visible');
    content.classList.remove('scale-in');
    setTimeout(() => modal.classList.remove('active'), 200);
}

function checkUrlHashForAutoJoin() {
    const hash = location.hash;
    if (!hash.startsWith('#join=')) return;
    const code = hash.replace('#join=', '').trim();
    if (!/^\d{4}$/.test(code)) return;
    $joinCodeInput.value = code;
    showToast(`Auto-join code ${code} detected — connecting…`, 'info');
    setTimeout(() => {
        handleJoin(null);
        history.replaceState(null, '', location.pathname);
    }, 1200);
}

// ─── COPY HELPERS ────────────────────────────────────────────────────────────
function copyRoomCode(btn) {
    navigator.clipboard.writeText(my4DigitCode)
        .then(() => { copyFeedback(btn, '<i class="fa-solid fa-check" aria-hidden="true"></i>'); showToast('Code copied!', 'success'); })
        .catch(() => showToast('Copy failed — check browser permissions.', 'error'));
}

function copyShareUrl(btn) {
    const url = id('shareUrlText').textContent;
    navigator.clipboard.writeText(url)
        .then(() => { copyFeedback(btn, '<i class="fa-solid fa-check" aria-hidden="true"></i> Copied!'); showToast('Link copied!', 'success'); })
        .catch(() => showToast('Copy failed.', 'error'));
}

// Safe copy — content passed as a live variable, never serialised into an attribute
function copyText(content, btn) {
    navigator.clipboard.writeText(content)
        .then(() => copyFeedback(btn, '<i class="fa-solid fa-check" aria-hidden="true"></i> Copied!'))
        .catch(() => showToast('Copy failed.', 'error'));
}

function copyFeedback(btn, html) {
    const orig = btn.innerHTML;
    btn.innerHTML = html;
    btn.classList.add('copy-success');
    setTimeout(() => { btn.innerHTML = orig; btn.classList.remove('copy-success'); }, 1800);
}

// ─── TOAST ───────────────────────────────────────────────────────────────────
function showToast(message, type = 'info') {
    const container = id('toastContainer');
    const toast     = document.createElement('div');
    toast.className = `toast toast-${type}`;

    const icons = { success:'fa-circle-check', error:'fa-circle-exclamation',
                    warning:'fa-triangle-exclamation', info:'fa-circle-info' };
    const icon  = icons[type] || 'fa-circle-info';

    // message goes via textContent — zero XSS risk
    const iconEl = document.createElement('i');
    iconEl.className = `fa-solid ${icon} toast-icon toast-icon-${type}`;
    iconEl.setAttribute('aria-hidden', 'true');

    const msgEl  = document.createElement('span');
    msgEl.className   = 'toast-msg';
    msgEl.textContent = message;

    toast.appendChild(iconEl);
    toast.appendChild(msgEl);
    container.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('toast-in'));
    setTimeout(() => {
        toast.classList.remove('toast-in');
        toast.classList.add('toast-out');
        setTimeout(() => toast.remove(), 300);
    }, 3500);
}

// ─── MICRO-INTERACTIONS ──────────────────────────────────────────────────────
function playNotificationPulse() {
    const h = document.querySelector('header');
    h.classList.add('header-pulse');
    setTimeout(() => h.classList.remove('header-pulse'), 800);
}

function playReceiveSound() {
    try {
        const ctx  = getAudioCtx();
        const osc  = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.type = 'sine';
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        osc.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.15);
        gain.gain.setValueAtTime(0.1, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3);
        osc.start(ctx.currentTime);
        osc.stop(ctx.currentTime + 0.3);
    } catch (_) {}
}

// ─── UTILITIES ───────────────────────────────────────────────────────────────
function pct(done, total) { return total > 0 ? Math.round((done / total) * 100) : 0; }

function fmtBytes(bytes) {
    if (!bytes || isNaN(bytes) || bytes < 0) return '0 B';
    if (bytes === 0) return '0 B';
    const k = 1024;
    const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), 4);
    return parseFloat((bytes / k ** i).toFixed(2)) + ' ' + ['B','KB','MB','GB','TB'][i];
}

// Display transfer speed in Mb/s (megabits per second) — the industry standard
// used by every speed test and ISP. 1 MB/s = 8 Mb/s, so numbers feel 8x larger
// and match what users expect to see ("I have 100 Mb/s internet").
function fmtSpeed(bytesPerSec) {
    if (!bytesPerSec || isNaN(bytesPerSec) || bytesPerSec <= 0) return '';
    const mbits = (bytesPerSec * 8) / (1000 * 1000); // bytes → megabits (SI)
    if (mbits >= 1000) return (mbits / 1000).toFixed(1) + ' Gb/s';
    if (mbits >= 1)    return mbits.toFixed(1) + ' Mb/s';
    const kbits = mbits * 1000;
    return kbits.toFixed(0) + ' Kb/s';
}

function fmtTime(ms) {
    if (!ms) return '';
    return new Date(ms).toLocaleTimeString([], { hour:'2-digit', minute:'2-digit', second:'2-digit' });
}

function fmtETA(s) { return s < 60 ? `${Math.ceil(s)}s` : `${Math.ceil(s / 60)}m`; }

function validUrl(str) {
    try { const u = new URL(str); return u.protocol === 'http:' || u.protocol === 'https:'; }
    catch (_) { return false; }
}

// Uses a cached element — never creates a new one per call
function esc(str) {
    _escEl.textContent = str;
    return _escEl.innerHTML;
}

function fileIcon(name) {
    const e = (name.split('.').pop() || '').toLowerCase();
    if (['jpg','jpeg','png','gif','svg','webp','avif','heic'].includes(e)) return 'fa-file-image';
    if (['mp4','mov','avi','webm','mkv','m4v'].includes(e))               return 'fa-file-video';
    if (['mp3','wav','ogg','flac','m4a','aac'].includes(e))               return 'fa-file-audio';
    if (e === 'pdf')                                                        return 'fa-file-pdf';
    if (['zip','rar','7z','tar','gz','bz2'].includes(e))                  return 'fa-file-zipper';
    if (['doc','docx','txt','md','rtf'].includes(e))                      return 'fa-file-lines';
    if (['xls','xlsx','csv'].includes(e))                                 return 'fa-file-csv';
    if (['js','ts','jsx','tsx','html','css','json','py','go','rs','c','cpp','java'].includes(e))
        return 'fa-file-code';
    return 'fa-file';
}

// ─── SHA-256 ─────────────────────────────────────────────────────────────────
// Uses DigestStream (Chrome 116+) for zero-RAM-spike streaming hash.
// Falls back to single-shot digest for files ≤ 256 MB.
// Returns null for files too large to hash without DigestStream — caller handles this.
async function computeFileHash(blob) {
    if (typeof DigestStream !== 'undefined') {
        try {
            const ds     = new DigestStream('SHA-256');
            const writer = ds.getWriter();
            let offset = 0;
            while (offset < blob.size) {
                const buf = await blob.slice(offset, offset + HASH_SLICE).arrayBuffer();
                await writer.write(new Uint8Array(buf));
                offset += HASH_SLICE;
            }
            await writer.close();
            return hexOf(new Uint8Array(await ds.digest));
        } catch (_) { /* fall through */ }
    }
    if (blob.size > HASH_FALLBACK_MAX) {
        console.warn('[AirFlux] File too large to hash without DigestStream — skipping checksum.');
        return null;
    }
    return hexOf(new Uint8Array(await crypto.subtle.digest('SHA-256', await blob.arrayBuffer())));
}

function hexOf(u8) {
    return Array.from(u8).map(b => b.toString(16).padStart(2, '0')).join('');
}

// ─── MOBILE TAB SWITCHING ─────────────────────────────────────────────────────
function switchTab(tab) {
    const sendPanel    = id('panel-send');
    const receivePanel = id('panel-receive');
    const tabSend      = id('tab-send');
    const tabReceive   = id('tab-receive');
    if (!sendPanel || !receivePanel) return;

    if (tab === 'send') {
        sendPanel.classList.remove('tab-hidden');
        receivePanel.classList.add('tab-hidden');
        tabSend?.classList.add('active');
        tabSend?.setAttribute('aria-selected', 'true');
        tabReceive?.classList.remove('active');
        tabReceive?.setAttribute('aria-selected', 'false');
    } else {
        receivePanel.classList.remove('tab-hidden');
        sendPanel.classList.add('tab-hidden');
        tabReceive?.classList.add('active');
        tabReceive?.setAttribute('aria-selected', 'true');
        tabSend?.classList.remove('active');
        tabSend?.setAttribute('aria-selected', 'false');
    }
}

// Auto-switch to Received tab when a new item arrives on mobile
function autoSwitchToReceive() {
    const isMobile = window.matchMedia('(max-width: 1023px)').matches;
    if (!isMobile) return;
    if (id('tab-receive')?.classList.contains('active')) return;
    switchTab('receive');
}

// ─── HOW-IT-WORKS STRIP ───────────────────────────────────────────────────────
const HOW_STRIP_KEY = 'airflux-how-dismissed';

function dismissHowStrip() {
    const strip = id('howStrip');
    if (!strip) return;
    strip.classList.add('dismissed');
    try { localStorage.setItem(HOW_STRIP_KEY, '1'); } catch (_) {}
}

(function initHowStrip() {
    try {
        if (localStorage.getItem(HOW_STRIP_KEY)) {
            const strip = id('howStrip');
            if (strip) strip.classList.add('dismissed');
        }
    } catch (_) {}
})();
