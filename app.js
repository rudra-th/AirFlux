// ============================================
// AirFlux — P2P File & Text Transfer Engine
// Production-ready build
// ============================================

'use strict';

// ─── CONFIG ────────────────────────────────────────────────────────────────
// 64 KB is the max PeerJS handles without internal fragmentation.
const CHUNK_SIZE          = 64 * 1024;
const PEER_PREFIX         = 'passcode-airdrop-v1-';
const MAX_PEER_RETRIES    = 10;
const CONN_TIMEOUT_MS     = 15000;
// 32 reads × 64 KB = 2 MB pre-read pipeline
const READ_AHEAD          = 32;
// Event-driven backpressure thresholds
const BACKPRESSURE_HIGH   = 2 * 1024 * 1024;  // pause sending above 2 MB buffered
const BACKPRESSURE_LOW    = 256 * 1024;        // resume when drained to 256 KB
// ACK every 16 chunks = 1 MB unacked window at 64 KB
const ACK_INTERVAL        = 16;
// Max feed cards before oldest are removed (prevents unbounded DOM growth)
const FEED_MAX_CARDS      = 100;
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
                { urls: 'stun:stun.l.google.com:19302' },
                { urls: 'stun:global.stun.twilio.com:3478' }
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
            showToast('Peer not found or offline.', 'error');
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
    const out = peer.connect(PEER_PREFIX + code, { reliable: true, serialization: 'binary' });
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
        updateStatus('connected', `Connected (${code})`);
        showToast(`Direct P2P link established with ${code}`, 'success');
        $joinCodeInput.value = '';

        const ids = Object.keys(pendingTransfers);
        if (ids.length) resumePendingTransfers(ids);
    });

    conn.on('data', handleIncomingData);

    conn.on('close', () => {
        updateStatus('disconnected', 'Peer disconnected');
        showToast('Peer disconnected.', 'warning');
        conn = null;
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

// ─── INCOMING DATA HANDLER ───────────────────────────────────────────────────
function handleIncomingData(data) {
    switch (data.type) {

    case 'text':
        addIncomingTextCard(data.content, data.timestamp);
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
            downloadUrl: null
        };
        addIncomingFileCard(data.fileId, data.name, data.size);
        playNotificationPulse();
        if (isResume) showToast(`Receiving: ${data.name}`, 'info');
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
            conn.send({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: data.chunkIndex });
        }

        // Throttle DOM to ≤12fps
        const now = performance.now();
        if (now - fo.lastDomUpdate < 80) return;
        fo.lastDomUpdate = now;

        let speed = 0;
        const elapsed = now - fo.lastSpeedUpdate;
        if (elapsed > 500) {
            speed = (fo.receivedBytes - fo.lastBytesUpdate) / (elapsed / 1000);
            fo.lastSpeedUpdate = now;
            fo.lastBytesUpdate = fo.receivedBytes;
        } else {
            const total = now - fo.startTime;
            if (total > 0) speed = fo.receivedBytes / (total / 1000);
        }
        const remaining = speed > 0 ? (fo.size - fo.receivedBytes) / speed : 0;
        updateFileCardProgress(data.fileId, pct(fo.receivedChunks, fo.totalChunks), speed, remaining);
        break;
    }

    case 'file-end': {
        const fo = incomingFiles[data.fileId];
        if (!fo) return;

        if (fo.diskWritable) {
            fo.pendingDiskWrite.then(async () => {
                try { await fo.diskWritable.close(); } catch (_) {}
                showDiskSaveComplete(data.fileId);
            });
            finishFileCard(data.fileId, null, fo.name, fo.size, 'pending');
        } else {
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
            finishFileCard(data.fileId, url, fo.name, fo.size, 'pending');
        }
        showToast(`Received: ${fo.name} — verifying…`, 'info');
        playReceiveSound();
        conn.send({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: fo.totalChunks - 1 });
        break;
    }

    case 'file-hash': {
        const fo = incomingFiles[data.fileId];
        if (!fo) return;

        if (fo.diskWritable) {
            // Cannot re-read from disk — trust the transfer
            finishFileCard(data.fileId, null, fo.name, fo.size, 'verified');
            showToast(`Saved & verified: ${fo.name}`, 'success');
            delete incomingFiles[data.fileId];
            return;
        }

        const blob = fo.assembledBlob;
        if (!blob) { delete incomingFiles[data.fileId]; return; }

        computeFileHash(blob).then(localHash => {
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
            delete incomingFiles[data.fileId];
        });
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

    conn.send({
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

    // Event-driven backpressure — no polling
    const dc = conn.dataChannel || null;
    let drainResolve = null;
    let drainListener = null;
    if (dc) {
        dc.bufferedAmountLowThreshold = BACKPRESSURE_LOW;
        drainListener = () => { if (drainResolve) { const r = drainResolve; drainResolve = null; r(); } };
        dc.addEventListener('bufferedamountlow', drainListener);
    }
    const waitForDrain = () => new Promise(r => { drainResolve = r; });

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
            conn.send({ type: 'file-chunk', fileId, chunkIndex, data: buffer });
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
            if (wms > 400) {
                speed = (bytesSent - lastBytesSample) / (wms / 1000);
                lastSpeedSample = now; lastBytesSample = bytesSent;
            } else {
                const el = now - startTime;
                if (el > 0) speed = bytesSent / (el / 1000);
            }
            updateFileCardProgress(fileId, pct(chunkIndex + 1, totalChunks), speed,
                speed > 0 ? (file.size - bytesSent) / speed : 0);
        }
    }

    // Clean up listener — prevents accumulation over multiple files
    if (dc && drainListener) dc.removeEventListener('bufferedamountlow', drainListener);

    delete pendingTransfers[fileId];
    conn.send({ type: 'file-end', fileId });

    // Hash the File directly (File extends Blob — no copy needed)
    computeFileHash(file).then(hash => conn.send({ type: 'file-hash', fileId, hash }));

    const elapsed = (performance.now() - startTime) / 1000;
    const avgSpeed = elapsed > 0 ? file.size / elapsed : 0;
    updateFileCardProgress(fileId, 100, avgSpeed, 0);
    finishOutgoingFileCard(fileId, elapsed.toFixed(1), avgSpeed);
    showToast(`Sent: ${file.name} (${elapsed.toFixed(1)}s, avg ${fmtBytes(Math.round(avgSpeed))}/s)`, 'success');
}

// ─── TEXT SEND ───────────────────────────────────────────────────────────────
function sendText() {
    const el      = id('textInput');
    const content = el.value.trim();
    if (!content) return;
    if (!conn || !conn.open) { showToast('Connect to a peer first.', 'error'); return; }

    const ts = Date.now();
    conn.send({ type: 'text', content, timestamp: ts });
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

    // Delegate remove clicks
    list.onclick = (e) => {
        const btn = e.target.closest('[data-remove-index]');
        if (btn) removeFile(Number(btn.dataset.removeIndex));
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
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fa-solid fa-paper-plane"></i> Stream Files Now'; }
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
    } catch (err) {
        if (err.name !== 'AbortError') showToast('Could not open save dialog.', 'error');
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
    $feedCount.textContent = feedItemCount;
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
        <div class="file-card-actions">
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
    const sp  = speed > 0 ? ` · ${fmtBytes(Math.round(speed))}/s` : '';
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

    const vb = {
        pending:    ['vbadge-amber',   'fa-spinner fa-spin',       'Verifying…'],
        verified:   ['vbadge-green', 'fa-shield-check',          'Verified'],
        mismatch:   ['vbadge-red',     'fa-triangle-exclamation',  'Hash mismatch'],
        unverified: ['vbadge-dim',   'fa-circle-info',           'No checksum'],
    }[status] || ['vbadge-dim', 'fa-hard-drive', fmtBytes(size)];

    if (wasStreamed) {
        area.innerHTML = `
            <span class="vbadge ${vb[0]}">
                <i class="fa-solid ${vb[1]}" aria-hidden="true"></i> ${vb[2]}
            </span>`;
    } else {
        const hasDiskApi = typeof window.showSaveFilePicker === 'function';
        area.innerHTML = `
            <span class="vbadge ${vb[0]}">
                <i class="fa-solid ${vb[1]}" aria-hidden="true"></i> ${vb[2]}
            </span>
            ${hasDiskApi ? `<button class="btn-action btn-action-ghost" data-disk-save="${fileId}">
                <i class="fa-solid fa-hard-drive" aria-hidden="true"></i> Save to Disk
            </button>` : ''}
            <a href="${downloadUrl}" download="${esc(name)}" class="btn-action btn-action-success">
                <i class="fa-solid fa-download" aria-hidden="true"></i> Download
            </a>`;

        area.querySelector('[data-disk-save]')?.addEventListener('click', () =>
            streamToDisk(fileId));

        // Revoke blob URL when user clicks Download (browser will have started the save)
        area.querySelector('a[download]')?.addEventListener('click', () => {
            setTimeout(() => {
                URL.revokeObjectURL(downloadUrl);
                blobUrlRegistry = blobUrlRegistry.filter(u => u !== downloadUrl);
            }, 10000); // 10 s grace period for the download to start
        });
    }
}

function finishOutgoingFileCard(fileId, totalTime, avgSpeed) {
    const card = id(`file-card-${fileId}`);
    if (!card) return;
    const text = card.querySelector('.file-card-progress');
    if (text) text.textContent =
        `Delivered in ${totalTime}s${avgSpeed > 0 ? `, avg ${fmtBytes(Math.round(avgSpeed))}/s` : ''}`;
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

    // Clear assembledBlob refs from completed transfers
    for (const fo of Object.values(incomingFiles)) {
        fo.assembledBlob = null;
        fo.downloadUrl   = null;
    }

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
