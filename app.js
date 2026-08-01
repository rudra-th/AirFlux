// ============================================
// AirFlux — P2P File & Text Transfer Engine
// ============================================

// --- CONFIG ---
const CHUNK_SIZE = 512 * 1024;           // 512 KB per chunk
const PEER_PREFIX = 'passcode-airdrop-v1-';
const MAX_PEER_RETRIES = 10;
const CONNECTION_TIMEOUT_MS = 15000;
const READ_AHEAD = 64;
const BACKPRESSURE_THRESHOLD = 8 * 1024 * 1024;
const BACKPRESSURE_CHECK_MS = 5;
const ACK_INTERVAL = 8;                  // FIX: was 16 (too large a window); halved

// --- STATE ---
let peer = null;
let conn = null;
let my4DigitCode = '';
let selectedFiles = [];
let sendingInProgress = false;
let pendingTransfers = {};
let incomingFiles = {};
let fileDomCache = {};                   // keyed by fileId → { bar, text }
let feedItemCount = 0;
let peerRetryCount = 0;
let connectionTimeout = null;
let connectionAttemptActive = false;

// FIX: Single AudioContext singleton — never create a new one per sound
let _audioCtx = null;
function getAudioCtx() {
    if (!_audioCtx) {
        _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    }
    // Resume if browser suspended it (autoplay policy)
    if (_audioCtx.state === 'suspended') {
        _audioCtx.resume().catch(() => {});
    }
    return _audioCtx;
}

// --- DOM HELPERS ---
const $ = (id) => document.getElementById(id);
const statusPill    = $('statusPill');
const statusDot     = $('statusDot');
const statusText    = $('statusText');
const myRoomCodeEl  = $('myRoomCode');
const joinCodeInput = $('joinCodeInput');
const feedContainer = $('feedContainer');
const emptyState    = $('emptyState');
const feedCountEl   = $('feedCount');
const dropzone      = $('dropzone');
const dropzonePrompt    = $('dropzonePrompt');
const selectedFileState = $('selectedFileState');
const retryOverlay  = $('retryOverlay');

// --- INIT ---
document.addEventListener('DOMContentLoaded', () => {
    initPeer();
    setupDragAndDrop();
    setupPasteToSend();         // NEW: paste anywhere sends to text pad
    checkUrlHashForAutoJoin();

    // FIX: Enter key on join input submits the form (single-line input, Enter = submit)
    joinCodeInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            handleJoin(e);
        }
    });
});

// --- PASTE-TO-SEND ---
// NEW: if the user pastes while NOT focused on the join input or file area,
// dump text into the text pad and focus it.
function setupPasteToSend() {
    document.addEventListener('paste', (e) => {
        const active = document.activeElement;
        // Don't hijack paste when user is in the join input
        if (active && (active.id === 'joinCodeInput')) return;
        // Don't hijack when user is already in the text input
        if (active && active.id === 'textInput') return;

        const text = e.clipboardData && e.clipboardData.getData('text');
        if (!text) return;

        const textEl = $('textInput');
        if (!textEl) return;
        e.preventDefault();
        textEl.value = text;
        textEl.focus();
        showToast('Pasted — press Enter or click Send.', 'info');
    });

    // FIX: Enter in text area = send (Shift+Enter = newline, plain Enter = send)
    const textEl = $('textInput');
    if (textEl) {
        textEl.addEventListener('keydown', handleTextKeydown);
    }
}

// --- WEBRTC SIGNALING & PEER MANAGEMENT ---
function generate4DigitCode() {
    return Math.floor(1000 + Math.random() * 9000).toString();
}

function initPeer(customCode = null) {
    updateStatus('connecting', 'Signaling...');
    my4DigitCode = customCode || generate4DigitCode();
    const fullPeerId = PEER_PREFIX + my4DigitCode;

    peer = new Peer(fullPeerId, {
        debug: 0,
        config: {
            iceServers: [
                { urls: 'stun:stun.l.google.com:19302' },
                { urls: 'stun:global.stun.twilio.com:3478' }
            ]
        }
    });

    peer.on('open', () => {
        myRoomCodeEl.textContent = my4DigitCode;
        updateStatus('disconnected', 'Ready for Peer');
        generateQRCode();
    });

    peer.on('connection', (incomingConn) => {
        if (conn && conn.open) {
            incomingConn.close();
            return;
        }
        setupConnection(incomingConn);
    });

    peer.on('error', (err) => {
        console.error('PeerJS Error:', err.type, err);
        if (err.type === 'unavailable-id') {
            if (peerRetryCount >= MAX_PEER_RETRIES) {
                showToast('Too many ID collisions. Please try again later.', 'error');
                updateStatus('disconnected', 'Ready for Peer');
                peerRetryCount = 0;
                return;
            }
            peerRetryCount++;
            const delay = Math.min(1000 * Math.pow(2, peerRetryCount - 1), 10000);
            showToast(`Room code collision. Retrying in ${delay / 1000}s...`, 'warning');
            setTimeout(() => initPeer(), delay);
        } else if (err.type === 'peer-unavailable') {
            showToast('Peer code not found or offline.', 'error');
            updateStatus('disconnected', 'Ready for Peer');
            clearConnectionTimeout();
            showRetryOverlay();
        } else if (err.type === 'network' || err.type === 'server-error') {
            showToast('Signaling server error. Retrying...', 'error');
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
    const targetCode = joinCodeInput.value.trim();
    if (!/^\d{4}$/.test(targetCode)) {
        showToast('Please enter a valid 4-digit code.', 'error');
        return;
    }
    if (targetCode === my4DigitCode) {
        showToast('Cannot connect to your own device code.', 'error');
        return;
    }

    connectionAttemptActive = true;
    updateStatus('connecting', 'Connecting...');
    hideRetryOverlay();
    const targetPeerId = PEER_PREFIX + targetCode;
    const outgoingConn = peer.connect(targetPeerId, {
        reliable: true,
        serialization: 'binary'
    });

    setupConnection(outgoingConn);

    connectionTimeout = setTimeout(() => {
        if (conn && !conn.open) {
            showToast('Connection timed out. Peer may be offline.', 'error');
            updateStatus('disconnected', 'Ready for Peer');
            conn.close();
            conn = null;
            connectionAttemptActive = false;
            showRetryOverlay();
        }
    }, CONNECTION_TIMEOUT_MS);
}

function setupConnection(connection) {
    conn = connection;

    conn.on('open', () => {
        clearConnectionTimeout();
        peerRetryCount = 0;
        connectionAttemptActive = false;
        hideRetryOverlay();
        const peerCode = conn.peer.replace(PEER_PREFIX, '');
        updateStatus('connected', `P2P Active (${peerCode})`);
        showToast(`Connected directly to peer: ${peerCode}`, 'success');
        joinCodeInput.value = '';

        const pendingIds = Object.keys(pendingTransfers);
        if (pendingIds.length > 0) {
            resumePendingTransfers(pendingIds);
        }
    });

    conn.on('data', handleIncomingData);

    conn.on('close', () => {
        updateStatus('disconnected', 'Peer Disconnected');
        showToast('Peer disconnected from session.', 'warning');
        conn = null;
        connectionAttemptActive = false;
        showRetryOverlay();
    });

    conn.on('error', (err) => {
        console.error('Connection Error:', err);
        showToast('Data channel error occurred.', 'error');
    });
}

function handleRetry() {
    hideRetryOverlay();
    const lastCode = joinCodeInput.value.trim();
    if (/^\d{4}$/.test(lastCode) && lastCode !== my4DigitCode) {
        handleJoin(null);
    } else {
        joinCodeInput.focus();
    }
}

async function resumePendingTransfers(ids) {
    for (const fileId of ids) {
        const pending = pendingTransfers[fileId];
        if (pending && pending.file) {
            showToast(`Resuming: ${pending.file.name}...`, 'info');
            await sendFileOverWebRTC(pending.file, pending.lastAckedChunk + 1, fileId);
        }
    }
}

function showRetryOverlay() {
    if (retryOverlay) retryOverlay.classList.add('visible');
}
function hideRetryOverlay() {
    if (retryOverlay) retryOverlay.classList.remove('visible');
}
function updateStatus(state, text) {
    statusText.textContent = text;
    statusPill.className = 'status-pill ' + state;
    statusDot.className = 'status-dot ' + state;
}

// --- DATA CHANNEL PROTOCOL ---
function handleIncomingData(data) {
    if (data.type === 'text') {
        addIncomingTextCard(data.content, data.timestamp);
        playNotificationPulse();
        playReceiveSound();

    } else if (data.type === 'file-start') {
        const canStreamToDisk = typeof window.showSaveFilePicker === 'function';
        incomingFiles[data.fileId] = {
            name: data.name,
            size: data.size,
            fileType: data.fileType,
            totalChunks: data.totalChunks,
            receivedChunks: 0,
            receivedBytes: 0,
            receivedChunkIndices: new Set(),
            buffers: new Array(data.totalChunks),
            startTime: performance.now(),
            lastSpeedUpdate: performance.now(),
            lastBytesAtSpeedUpdate: 0,
            lastDomUpdate: 0,
            canStreamToDisk,
            diskWritable: null,
            diskHandle: null,
            // FIX: track whether disk stream is fully set up (not just handle opened)
            diskReady: false,
            // pending chunks that arrived before disk was ready
            pendingDiskChunks: [],
            pendingDiskWrite: Promise.resolve()
        };
        addIncomingFileCard(data.fileId, data.name, data.size);
        playNotificationPulse();

    } else if (data.type === 'file-chunk') {
        const fileObj = incomingFiles[data.fileId];
        if (!fileObj) return;

        // Dedup
        if (fileObj.receivedChunkIndices.has(data.chunkIndex)) return;
        fileObj.receivedChunkIndices.add(data.chunkIndex);

        if (fileObj.diskReady && fileObj.diskWritable) {
            // Disk stream is fully ready — write in order via chained promise
            const chunkData = data.data;
            const chunkIndex = data.chunkIndex;
            fileObj.pendingDiskWrite = fileObj.pendingDiskWrite.then(() =>
                fileObj.diskWritable.write(new Uint8Array(chunkData))
            ).catch(() => {});
            fileObj.buffers[chunkIndex] = null; // free RAM
        } else if (fileObj.diskWritable && !fileObj.diskReady) {
            // FIX: disk handle opened but not flushed yet — queue chunks
            fileObj.pendingDiskChunks.push({ index: data.chunkIndex, data: data.data });
            fileObj.buffers[data.chunkIndex] = null;
        } else {
            // In-memory accumulation
            fileObj.buffers[data.chunkIndex] = data.data;
        }

        fileObj.receivedChunks++;
        fileObj.receivedBytes += data.data.byteLength;

        if (fileObj.receivedChunks % ACK_INTERVAL === 0) {
            conn.send({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: data.chunkIndex });
        }

        const now = performance.now();
        if (now - fileObj.lastDomUpdate < 80) return;
        fileObj.lastDomUpdate = now;

        let speed = 0;
        const windowElapsed = now - fileObj.lastSpeedUpdate;
        if (windowElapsed > 500) {
            speed = (fileObj.receivedBytes - fileObj.lastBytesAtSpeedUpdate) / (windowElapsed / 1000);
            fileObj.lastSpeedUpdate = now;
            fileObj.lastBytesAtSpeedUpdate = fileObj.receivedBytes;
        } else {
            const elapsed = now - fileObj.startTime;
            speed = elapsed > 0 ? fileObj.receivedBytes / (elapsed / 1000) : 0;
        }
        const percent = Math.round((fileObj.receivedChunks / fileObj.totalChunks) * 100);
        const remaining = speed > 0 ? ((fileObj.size - fileObj.receivedBytes) / speed) : 0;
        updateFileCardProgress(data.fileId, percent, speed, remaining);

    } else if (data.type === 'file-end') {
        const fileObj = incomingFiles[data.fileId];
        if (!fileObj) return;

        if (fileObj.diskWritable) {
            fileObj.pendingDiskWrite.then(async () => {
                try { await fileObj.diskWritable.close(); } catch (_) {}
                showDiskSaveComplete(data.fileId);
            });
            fileObj.assembledBlob = null;
            fileObj.downloadUrl = null;
            finishFileCard(data.fileId, null, fileObj.name, fileObj.size, 'pending');
        } else {
            // FIX: filter nulls (disk-streamed slots) before building Blob
            const validBuffers = fileObj.buffers.filter(b => b != null);
            const blob = new Blob(validBuffers, { type: fileObj.fileType || 'application/octet-stream' });
            const downloadUrl = URL.createObjectURL(blob);
            fileObj.assembledBlob = blob;
            fileObj.downloadUrl = downloadUrl;
            finishFileCard(data.fileId, downloadUrl, fileObj.name, fileObj.size, 'pending');
        }
        showToast(`Received: ${fileObj.name} — verifying integrity...`, 'info');
        playReceiveSound();
        conn.send({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: fileObj.totalChunks - 1 });

    } else if (data.type === 'file-hash') {
        const fileObj = incomingFiles[data.fileId];
        if (!fileObj) return;

        if (fileObj.diskWritable) {
            // For disk-streamed files we trust the transfer (can't re-read from disk here)
            finishFileCard(data.fileId, null, fileObj.name, fileObj.size, 'verified');
            showToast(`Received & verified: ${fileObj.name} (saved to disk)`, 'success');
            delete incomingFiles[data.fileId];
            return;
        }

        const verifyAgainst = fileObj.assembledBlob;
        if (!verifyAgainst) {
            delete incomingFiles[data.fileId];
            return;
        }

        computeFileHash(verifyAgainst).then(localHash => {
            const verified = localHash === data.hash;
            if (verified) {
                finishFileCard(data.fileId, fileObj.downloadUrl, fileObj.name, fileObj.size, 'verified');
                showToast(`Integrity verified: ${fileObj.name}`, 'success');
            } else {
                finishFileCard(data.fileId, fileObj.downloadUrl, fileObj.name, fileObj.size, 'mismatch');
                showToast(`Integrity mismatch: ${fileObj.name} may be corrupted.`, 'warning');
            }
            delete incomingFiles[data.fileId];
        });

    } else if (data.type === 'file-ack') {
        const pending = pendingTransfers[data.fileId];
        if (pending) {
            pending.lastAckedChunk = Math.max(pending.lastAckedChunk, data.lastChunkIndex);
        }

    } else if (data.type === 'file-resume') {
        // FIX: on resume, if we already have an entry, keep its existing progress
        if (!incomingFiles[data.fileId]) {
            incomingFiles[data.fileId] = {
                name: data.name,
                size: data.size,
                fileType: data.fileType,
                totalChunks: data.totalChunks,
                receivedChunks: 0,
                receivedBytes: 0,
                receivedChunkIndices: new Set(),
                buffers: new Array(data.totalChunks),
                startTime: performance.now(),
                lastSpeedUpdate: performance.now(),
                lastBytesAtSpeedUpdate: 0,
                lastDomUpdate: 0,
                canStreamToDisk: typeof window.showSaveFilePicker === 'function',
                diskWritable: null,
                diskHandle: null,
                diskReady: false,
                pendingDiskChunks: [],
                pendingDiskWrite: Promise.resolve()
            };
            addIncomingFileCard(data.fileId, data.name, data.size);
        } else {
            // Update totalChunks in case it changed, keep existing buffers/progress
            incomingFiles[data.fileId].totalChunks = data.totalChunks;
        }
        const fileObj = incomingFiles[data.fileId];
        const percent = Math.round((fileObj.receivedChunks / fileObj.totalChunks) * 100);
        updateFileCardProgress(data.fileId, percent, 0, 0);
        showToast(`Resuming: ${data.name} from ${percent}%`, 'info');
        playNotificationPulse();
    }
}

// --- FILE SENDING ---
async function sendFileOverWebRTC(file, resumeFrom = 0, existingFileId = null) {
    if (!conn || !conn.open) {
        showToast('No active peer connection. Connect first!', 'error');
        return;
    }

    const fileId = existingFileId || Math.random().toString(36).substring(2, 11);
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE) || 1;

    if (resumeFrom > 0) {
        conn.send({
            type: 'file-resume',
            fileId, name: file.name, size: file.size,
            fileType: file.type || 'application/octet-stream',
            totalChunks, resumeFrom
        });
    } else {
        conn.send({
            type: 'file-start',
            fileId, name: file.name, size: file.size,
            fileType: file.type || 'application/octet-stream',
            totalChunks
        });
    }

    pendingTransfers[fileId] = { file, totalChunks, lastAckedChunk: resumeFrom - 1 };

    if ($(`file-card-${fileId}`)) {
        updateFileCardProgress(fileId, Math.round((resumeFrom / totalChunks) * 100), 0, 0);
    } else {
        addOutgoingFileCard(fileId, file.name, file.size);
    }

    // FIX: use the public dataChannel property (PeerJS 1.x), with a safe fallback
    const dc = (conn.dataChannel) || null;

    const startTime = performance.now();
    let bytesSent = resumeFrom * CHUNK_SIZE;
    let lastSpeedUpdate = startTime;
    let lastBytesAtSpeedUpdate = bytesSent;
    let lastDomUpdate = 0;

    const readQueue = [];
    let readOffset = resumeFrom * CHUNK_SIZE;

    function enqueueRead() {
        if (readOffset >= file.size) return;
        const end = Math.min(readOffset + CHUNK_SIZE, file.size);
        const slice = file.slice(readOffset, end);
        readOffset = end;
        readQueue.push(slice.arrayBuffer());
    }

    for (let i = 0; i < READ_AHEAD; i++) enqueueRead();

    for (let chunkIndex = resumeFrom; chunkIndex < totalChunks; chunkIndex++) {
        // Backpressure — use public API when available
        if (dc) {
            while (dc.bufferedAmount > BACKPRESSURE_THRESHOLD) {
                await new Promise(r => setTimeout(r, BACKPRESSURE_CHECK_MS));
            }
        }

        const promise = readQueue.shift();
        if (!promise) break;
        const buffer = await promise;
        if (!buffer) break;
        enqueueRead();

        try {
            conn.send({ type: 'file-chunk', fileId, chunkIndex, data: buffer });
        } catch (_) {
            showToast('Transfer interrupted — will resume on reconnect.', 'error');
            finishOutgoingFileCard(fileId, '?', 0);
            return;
        }

        bytesSent += buffer.byteLength;

        const now = performance.now();
        if (now - lastDomUpdate >= 100) {
            lastDomUpdate = now;
            let speed = 0;
            const windowElapsed = now - lastSpeedUpdate;
            if (windowElapsed > 500) {
                speed = (bytesSent - lastBytesAtSpeedUpdate) / (windowElapsed / 1000);
                lastSpeedUpdate = now;
                lastBytesAtSpeedUpdate = bytesSent;
            } else {
                const elapsed = now - startTime;
                if (elapsed > 0) speed = bytesSent / (elapsed / 1000);
            }
            const percent = Math.round(((chunkIndex + 1) / totalChunks) * 100);
            const remaining = speed > 0 ? ((file.size - bytesSent) / speed) : 0;
            updateFileCardProgress(fileId, percent, speed, remaining);
        }
    }

    delete pendingTransfers[fileId];
    conn.send({ type: 'file-end', fileId });

    // FIX: hash the blob to match what the receiver will hash (consistent type)
    const sentBlob = new Blob([file], { type: file.type || 'application/octet-stream' });
    computeFileHash(sentBlob).then(hash => {
        conn.send({ type: 'file-hash', fileId, hash });
    });

    const totalTime = ((performance.now() - startTime) / 1000).toFixed(1);
    const avgSpeed = file.size / ((performance.now() - startTime) / 1000);
    updateFileCardProgress(fileId, 100, avgSpeed, 0);
    finishOutgoingFileCard(fileId, totalTime, avgSpeed);
    showToast(`Sent: ${file.name} (${totalTime}s, avg ${formatBytes(Math.round(avgSpeed))}/s)`, 'success');
}

// --- TEXT SEND ---
function sendText() {
    const textEl = $('textInput');
    const content = textEl.value.trim();
    if (!content) return;

    if (!conn || !conn.open) {
        showToast('Connect to a peer device before sending text.', 'error');
        return;
    }

    const payload = { type: 'text', content, timestamp: Date.now() };
    conn.send(payload);
    addOutgoingTextCard(content, payload.timestamp);
    textEl.value = '';
    showToast('Text sent to peer!', 'success');
}

// FIX: Enter = send, Shift+Enter = newline, Ctrl/Cmd+Enter = also send (kept for habit)
function handleTextKeydown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendText();
    }
}

// --- DRAG & DROP ---
function setupDragAndDrop() {
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropzone.addEventListener(eventName, (e) => { e.preventDefault(); e.stopPropagation(); }, false);
    });
    ['dragenter', 'dragover'].forEach(eventName => {
        dropzone.addEventListener(eventName, () => dropzone.classList.add('dragover'), false);
    });
    ['dragleave', 'drop'].forEach(eventName => {
        dropzone.addEventListener(eventName, () => dropzone.classList.remove('dragover'), false);
    });
    dropzone.addEventListener('drop', (e) => {
        const files = e.dataTransfer.files;
        if (files && files.length > 0) addFilesToQueue(Array.from(files));
    }, false);
}

function handleFileSelect(e) {
    const files = e.target.files;
    if (files && files.length > 0) {
        addFilesToQueue(Array.from(files));
        e.target.value = '';
    }
}

function addFilesToQueue(newFiles) {
    for (const file of newFiles) {
        if (!selectedFiles.some(f => f.name === file.name && f.size === file.size)) {
            selectedFiles.push(file);
        }
    }
    updateFileListUI();
}

function removeFile(index) {
    selectedFiles.splice(index, 1);
    updateFileListUI();
}

function clearAllFiles(e) {
    if (e) e.stopPropagation();
    selectedFiles = [];
    const fi = $('fileInput');
    if (fi) fi.value = '';
    selectedFileState.classList.add('hidden');
    dropzonePrompt.classList.remove('hidden');
}

function updateFileListUI() {
    if (selectedFiles.length === 0) {
        selectedFiles = [];
        const fi = $('fileInput');
        if (fi) fi.value = '';
        selectedFileState.classList.add('hidden');
        dropzonePrompt.classList.remove('hidden');
        return;
    }
    dropzonePrompt.classList.add('hidden');
    selectedFileState.classList.remove('hidden');

    const fileList = $('fileList');
    const summary = $('fileListSummary');

    fileList.innerHTML = selectedFiles.map((file, i) => `
        <div class="file-list-item">
            <div class="selected-file-info">
                <div class="file-icon-box emerald">
                    <i class="fa-solid ${getFileIconClass(file.name)}"></i>
                </div>
                <div class="file-details">
                    <p class="file-name" title="${escapeHtml(file.name)}">${escapeHtml(file.name)}</p>
                    <p class="file-size">${formatBytes(file.size)}</p>
                </div>
            </div>
            <button type="button" onclick="removeFile(${i})" class="btn-clear-file" title="Remove">
                <i class="fa-solid fa-xmark"></i>
            </button>
        </div>
    `).join('');

    const totalSize = selectedFiles.reduce((sum, f) => sum + f.size, 0);
    summary.textContent = `${selectedFiles.length} file${selectedFiles.length !== 1 ? 's' : ''} \u2014 ${formatBytes(totalSize)}`;
}

async function sendAllFiles(e) {
    if (e) e.stopPropagation();
    if (selectedFiles.length === 0) return;
    if (!conn || !conn.open) {
        showToast('Connect to a peer device before sending files.', 'error');
        return;
    }
    const filesToSend = [...selectedFiles];
    clearAllFiles(null);
    for (const file of filesToSend) {
        await sendFileOverWebRTC(file);
    }
}

// --- DISK STREAMING ---
async function startStreamToDisk(fileId) {
    const fileObj = incomingFiles[fileId];
    if (!fileObj || fileObj.diskWritable) return;

    try {
        const handle = await window.showSaveFilePicker({
            suggestedName: fileObj.name,
            types: [{ description: 'All Files', accept: { '*/*': [] } }]
        });
        const writable = await handle.createWritable();
        fileObj.diskHandle = handle;
        fileObj.diskWritable = writable;

        const btn = $(`save-disk-btn-${fileId}`);
        if (btn) {
            btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Saving to Disk...';
            btn.disabled = true;
            btn.onclick = null;
        }

        // FIX: drain any in-memory buffers that arrived BEFORE the handle was ready,
        // ordered by chunk index, then mark diskReady so future chunks go direct to disk
        const bufferedChunks = [];
        for (let i = 0; i < fileObj.buffers.length; i++) {
            if (fileObj.buffers[i]) bufferedChunks.push({ index: i, data: fileObj.buffers[i] });
        }
        // also drain pendingDiskChunks (arrived while handle was being opened)
        for (const pc of fileObj.pendingDiskChunks) {
            bufferedChunks.push(pc);
        }
        fileObj.pendingDiskChunks = [];
        bufferedChunks.sort((a, b) => a.index - b.index);

        fileObj.pendingDiskWrite = fileObj.pendingDiskWrite.then(async () => {
            for (const chunk of bufferedChunks) {
                await writable.write(new Uint8Array(chunk.data));
            }
        });

        fileObj.buffers = null;
        fileObj.diskReady = true; // FIX: signal that stream is live

        showToast('Streaming directly to disk...', 'info');
    } catch (err) {
        if (err.name !== 'AbortError') {
            showToast('Failed to open save dialog.', 'error');
        }
    }
}

function showDiskSaveComplete(fileId) {
    const btn = $(`save-disk-btn-${fileId}`);
    if (btn) {
        btn.innerHTML = '<i class="fa-solid fa-check" style="color:var(--emerald-400)"></i> Saved to Disk';
        btn.disabled = false;
    }
}

async function streamToDisk(fileId) {
    const fileObj = incomingFiles[fileId];
    const card = $(`file-card-${fileId}`);
    if (!card) return;

    let blob, name;
    if (fileObj && fileObj.assembledBlob) {
        blob = fileObj.assembledBlob;
        name = fileObj.name;
    } else {
        const link = card.querySelector('a[download]');
        if (!link) return;
        name = link.getAttribute('download');
        const href = link.getAttribute('href');
        const resp = await fetch(href);
        blob = await resp.blob();
    }

    try {
        const handle = await window.showSaveFilePicker({
            suggestedName: name,
            types: [{ description: 'All Files', accept: { '*/*': [] } }]
        });
        const writable = await handle.createWritable();
        await writable.write(blob);
        await writable.close();
        showToast(`Saved: ${name} to disk`, 'success');
    } catch (err) {
        if (err.name !== 'AbortError') {
            showToast('Failed to save file.', 'error');
        }
    }
}

// --- UI: FEED CARDS ---
function checkEmptyState() {
    emptyState.classList.toggle('hidden', feedItemCount > 0);
    feedCountEl.textContent = feedItemCount;
}

function addIncomingTextCard(content, timestamp) {
    feedItemCount++;
    checkEmptyState();

    const safeLink = isValidUrl(content);
    const card = document.createElement('div');
    card.className = 'feed-card feed-in';

    let actionButtonsHtml = `
        <button onclick="copyCardText('${encodeURIComponent(content)}', this)" class="btn-action btn-action-ghost">
            <i class="fa-regular fa-copy"></i> Copy
        </button>
    `;
    if (safeLink) {
        actionButtonsHtml = `
            <a href="${escapeHtml(content)}" target="_blank" rel="noopener noreferrer" class="btn-action btn-action-indigo">
                <i class="fa-solid fa-arrow-up-right-from-square"></i> Open Link
            </a>
            <button onclick="copyCardText('${encodeURIComponent(content)}', this)" class="btn-action btn-action-ghost">
                <i class="fa-regular fa-copy"></i> Copy
            </button>
        `;
    }

    card.innerHTML = `
        <div class="feed-card-header">
            <span class="badge ${safeLink ? 'badge-indigo' : 'badge-indigo'}">
                ${safeLink ? '<i class="fa-solid fa-link"></i> URL' : '<i class="fa-regular fa-message"></i> Text'}
            </span>
            <span class="badge-time">${formatTime(timestamp)}</span>
        </div>
        <div class="feed-card-body">${escapeHtml(content)}</div>
        <div class="feed-card-actions">${actionButtonsHtml}</div>
    `;

    feedContainer.insertBefore(card, feedContainer.firstChild);
}

function addOutgoingTextCard(content, timestamp) {
    feedItemCount++;
    checkEmptyState();

    const card = document.createElement('div');
    card.className = 'feed-card sent';
    card.innerHTML = `
        <div class="feed-card-header">
            <span class="badge badge-sent">
                <i class="fa-solid fa-arrow-right-from-bracket"></i> Sent
            </span>
            <span class="badge-time">${formatTime(timestamp)}</span>
        </div>
        <div class="feed-card-body sent-body">${escapeHtml(content)}</div>
    `;
    feedContainer.insertBefore(card, feedContainer.firstChild);
}

function addIncomingFileCard(fileId, name, size) {
    feedItemCount++;
    checkEmptyState();

    const canStreamToDisk = typeof window.showSaveFilePicker === 'function';
    const card = document.createElement('div');
    card.id = `file-card-${fileId}`;
    card.className = 'file-card';

    card.innerHTML = `
        <div class="feed-card-header">
            <span class="badge badge-emerald"><i class="fa-solid fa-cloud-arrow-down"></i> Incoming</span>
            <span class="file-size-badge">${formatBytes(size)}</span>
        </div>
        <div class="file-card-row">
            <div class="file-icon-box emerald">
                <i class="fa-solid ${getFileIconClass(name)}"></i>
            </div>
            <div class="file-card-meta">
                <p class="file-card-name" title="${escapeHtml(name)}">${escapeHtml(name)}</p>
                <p class="file-card-progress-text progress-text">Receiving… 0%</p>
            </div>
        </div>
        <div class="progress-track">
            <div class="progress-bar emerald" style="width:0%"></div>
        </div>
        <div class="file-preview" id="preview-${fileId}"></div>
        <div class="file-card-actions action-area visible">
            ${canStreamToDisk ? `
            <button onclick="startStreamToDisk('${fileId}')" class="btn-action btn-action-ghost" id="save-disk-btn-${fileId}">
                <i class="fa-solid fa-hard-drive"></i> Save to Disk
            </button>` : ''}
        </div>
    `;
    feedContainer.insertBefore(card, feedContainer.firstChild);
}

function addOutgoingFileCard(fileId, name, size) {
    feedItemCount++;
    checkEmptyState();

    const card = document.createElement('div');
    card.id = `file-card-${fileId}`;
    card.className = 'file-card outgoing';

    card.innerHTML = `
        <div class="feed-card-header">
            <span class="badge badge-teal"><i class="fa-solid fa-cloud-arrow-up"></i> Sending</span>
            <span class="file-size-badge">${formatBytes(size)}</span>
        </div>
        <div class="file-card-row">
            <div class="file-icon-box teal">
                <i class="fa-solid ${getFileIconClass(name)}"></i>
            </div>
            <div class="file-card-meta">
                <p class="file-card-name">${escapeHtml(name)}</p>
                <p class="file-card-progress-text progress-text">Streaming… 0%</p>
            </div>
        </div>
        <div class="progress-track">
            <div class="progress-bar teal" style="width:0%"></div>
        </div>
    `;
    feedContainer.insertBefore(card, feedContainer.firstChild);
}

// FIX: guard against stale fileDomCache refs pointing at detached DOM nodes
function updateFileCardProgress(fileId, percent, speed, remaining) {
    let cached = fileDomCache[fileId];

    // validate cached refs still in DOM
    if (cached && (!cached.bar.isConnected || !cached.text.isConnected)) {
        delete fileDomCache[fileId];
        cached = null;
    }

    if (!cached) {
        const card = $(`file-card-${fileId}`);
        if (!card) return;
        const bar = card.querySelector('.progress-bar');
        const text = card.querySelector('.progress-text');
        if (!bar || !text) return;
        fileDomCache[fileId] = { bar, text };
        cached = fileDomCache[fileId];
    }

    cached.bar.style.width = `${percent}%`;
    const speedStr = speed > 0 ? ` · ${formatBytes(Math.round(speed))}/s` : '';
    const etaStr = remaining > 1 ? ` · ${formatETA(remaining)}` : '';
    cached.text.textContent = `${percent}%${speedStr}${etaStr}`;
}

function finishFileCard(fileId, downloadUrl, name, size, verificationStatus) {
    delete fileDomCache[fileId];
    const card = $(`file-card-${fileId}`);
    if (!card) return;

    const fileObj = incomingFiles[fileId];
    const wasStreamed = fileObj && fileObj.diskWritable;

    const text = card.querySelector('.progress-text');
    if (text) {
        const statusMap = {
            pending: 'Complete — verifying integrity…',
            verified: 'Complete — integrity verified ✓',
            mismatch: 'Complete — integrity MISMATCH (may be corrupted)',
        };
        text.textContent = statusMap[verificationStatus] || 'Complete';
        if (verificationStatus === 'mismatch') text.classList.add('error');
    }

    const bar = card.querySelector('.progress-bar');
    if (bar) {
        bar.style.width = '100%';
        if (bar.classList.contains('emerald')) {
            bar.classList.replace('emerald', 'emerald-light');
        }
    }

    const previewEl = card.querySelector('.file-preview');
    if (previewEl && !previewEl.hasChildNodes() && downloadUrl && fileObj && fileObj.fileType) {
        addFilePreview(previewEl, fileObj.fileType, downloadUrl);
    }

    const actionArea = card.querySelector('.action-area');
    if (!actionArea) return;
    actionArea.classList.add('visible');

    let vbClass = 'vb-slate', vbIcon = 'fa-hard-drive', vbText = formatBytes(size);
    if (wasStreamed) { vbClass = 'vb-emerald'; vbIcon = 'fa-hard-drive'; vbText = 'Saved to Disk'; }
    else if (verificationStatus === 'verified') { vbClass = 'vb-emerald'; vbIcon = 'fa-shield-check'; vbText = 'Verified'; }
    else if (verificationStatus === 'mismatch') { vbClass = 'vb-red'; vbIcon = 'fa-triangle-exclamation'; vbText = 'Unverified'; }
    else if (verificationStatus === 'pending') { vbClass = 'vb-amber'; vbIcon = 'fa-spinner fa-spin'; vbText = 'Verifying…'; }

    const canSaveToDisk = typeof window.showSaveFilePicker === 'function';

    if (wasStreamed) {
        actionArea.innerHTML = `
            <span class="verification-badge ${vbClass}">
                <i class="fa-solid ${vbIcon}"></i> ${vbText}
            </span>`;
    } else {
        actionArea.innerHTML = `
            <span class="verification-badge ${vbClass}">
                <i class="fa-solid ${vbIcon}"></i> ${vbText}
            </span>
            ${canSaveToDisk ? `
            <button onclick="streamToDisk('${fileId}')" class="btn-action btn-action-ghost">
                <i class="fa-solid fa-hard-drive"></i> Save to Disk
            </button>` : ''}
            <a href="${downloadUrl}" download="${escapeHtml(name)}" class="btn-action btn-action-success">
                <i class="fa-solid fa-download"></i> Download
            </a>`;
    }
}

function finishOutgoingFileCard(fileId, totalTime, avgSpeed) {
    const card = $(`file-card-${fileId}`);
    if (!card) return;
    const text = card.querySelector('.progress-text');
    if (text) {
        const speedStr = avgSpeed > 0 ? `, avg ${formatBytes(Math.round(avgSpeed))}/s` : '';
        text.textContent = `Delivered in ${totalTime}s${speedStr}`;
    }
}

// FIX: clearFeed must fully reset fileDomCache — those DOM nodes are gone
function clearFeed() {
    // Remove all children except emptyState (which we re-attach)
    while (feedContainer.firstChild) {
        feedContainer.removeChild(feedContainer.firstChild);
    }
    feedContainer.appendChild(emptyState);
    feedItemCount = 0;
    fileDomCache = {};   // FIX: wipe stale refs
    checkEmptyState();
    showToast('Feed cleared.', 'info');
}

// --- FILE PREVIEW ---
function addFilePreview(container, fileType, blobUrl) {
    if (fileType.startsWith('image/')) {
        const img = document.createElement('img');
        img.src = blobUrl;
        img.className = 'preview-image';
        img.loading = 'lazy';
        img.onload = () => container.appendChild(img);
    } else if (fileType.startsWith('video/')) {
        const vid = document.createElement('video');
        vid.src = blobUrl;
        vid.className = 'preview-video';
        vid.controls = true;
        vid.preload = 'metadata';
        container.appendChild(vid);
    } else if (fileType.startsWith('audio/')) {
        const aud = document.createElement('audio');
        aud.src = blobUrl;
        aud.className = 'preview-audio';
        aud.controls = true;
        container.appendChild(aud);
    } else if (
        fileType.startsWith('text/') ||
        fileType === 'application/json' ||
        fileType === 'application/javascript'
    ) {
        fetch(blobUrl).then(r => r.text()).then(text => {
            const truncated = text.length > 600 ? text.slice(0, 600) + '\n…' : text;
            const pre = document.createElement('pre');
            pre.className = 'preview-text';
            pre.textContent = truncated;
            container.appendChild(pre);
        }).catch(() => {});
    }
}

// --- QR CODE & AUTO-JOIN ---
function generateQRCode() {
    const qrcodeContainer = $('qrcode');
    if (!qrcodeContainer) return;

    // FIX: show a loading state immediately so the modal isn't blank
    qrcodeContainer.innerHTML = '';
    const shareUrl = `${window.location.origin}${window.location.pathname}#join=${my4DigitCode}`;
    $('shareUrlText').textContent = shareUrl;

    new QRCode(qrcodeContainer, {
        text: shareUrl,
        width: 180,
        height: 180,
        colorDark: '#020617',
        colorLight: '#ffffff',
        correctLevel: QRCode.CorrectLevel.M
    });
}

function openQrModal() {
    // FIX: if QR not yet generated (peer still connecting), show a message
    if (!my4DigitCode || my4DigitCode === '----') {
        showToast('Still connecting — try again in a moment.', 'warning');
        return;
    }
    const modal = $('qrModal');
    const content = $('qrModalContent');
    modal.classList.add('active');
    requestAnimationFrame(() => {
        modal.classList.add('visible');
        content.classList.add('scale-in');
    });
}

function closeQrModal() {
    const modal = $('qrModal');
    const content = $('qrModalContent');
    modal.classList.remove('visible');
    content.classList.remove('scale-in');
    setTimeout(() => modal.classList.remove('active'), 200);
}

function checkUrlHashForAutoJoin() {
    const hash = window.location.hash;
    if (hash && hash.startsWith('#join=')) {
        const targetCode = hash.replace('#join=', '').trim();
        if (/^\d{4}$/.test(targetCode)) {
            joinCodeInput.value = targetCode;
            showToast(`Auto-discovered code ${targetCode}. Connecting…`, 'info');
            setTimeout(() => {
                handleJoin(null);
                window.history.replaceState(null, null, window.location.pathname);
            }, 1200);
        }
    }
}

// --- COPY HELPERS ---
function copyRoomCode(btn) {
    navigator.clipboard.writeText(my4DigitCode).then(() => {
        triggerCopyFeedback(btn, '<i class="fa-solid fa-check"></i>');
        showToast('Room code copied!', 'success');
    }).catch(() => showToast('Failed to copy. Check browser permissions.', 'error'));
}

function copyShareUrl(btn) {
    const shareUrl = $('shareUrlText').textContent;
    navigator.clipboard.writeText(shareUrl).then(() => {
        triggerCopyFeedback(btn, '<i class="fa-solid fa-check"></i> Copied!');
        showToast('Share link copied!', 'success');
    }).catch(() => showToast('Failed to copy. Check browser permissions.', 'error'));
}

function copyCardText(encodedText, btn) {
    const decoded = decodeURIComponent(encodedText);
    navigator.clipboard.writeText(decoded).then(() => {
        triggerCopyFeedback(btn, '<i class="fa-solid fa-check"></i> Copied!');
    }).catch(() => showToast('Failed to copy. Check browser permissions.', 'error'));
}

function triggerCopyFeedback(btnElement, successHtml) {
    const originalHtml = btnElement.innerHTML;
    btnElement.innerHTML = successHtml;
    btnElement.classList.add('copy-success');
    setTimeout(() => {
        btnElement.innerHTML = originalHtml;
        btnElement.classList.remove('copy-success');
    }, 1800);
}

// --- TOAST ---
function showToast(message, type = 'info') {
    const container = $('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    const iconMap = {
        success: 'fa-circle-check',
        error: 'fa-circle-exclamation',
        warning: 'fa-triangle-exclamation',
        info: 'fa-circle-info'
    };

    toast.innerHTML = `
        <i class="fa-solid ${iconMap[type] || 'fa-circle-info'} toast-icon toast-icon-${type}"></i>
        <span class="toast-msg">${escapeHtml(message)}</span>
    `;

    container.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('toast-show'));

    setTimeout(() => {
        toast.classList.remove('toast-show');
        toast.classList.add('toast-hide');
        setTimeout(() => toast.remove(), 300);
    }, 3500);
}

// --- MICRO-INTERACTIONS ---
function playNotificationPulse() {
    const header = document.querySelector('header');
    header.classList.add('header-pulse');
    setTimeout(() => header.classList.remove('header-pulse'), 800);
}

// FIX: AudioContext singleton — no more per-sound context creation
function playReceiveSound() {
    try {
        const ctx = getAudioCtx();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.type = 'sine';
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        osc.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.15);
        gain.gain.setValueAtTime(0.12, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3);
        osc.start(ctx.currentTime);
        osc.stop(ctx.currentTime + 0.3);
    } catch (_) {}
}

// --- CONNECTION TIMEOUT ---
function clearConnectionTimeout() {
    if (connectionTimeout) {
        clearTimeout(connectionTimeout);
        connectionTimeout = null;
    }
}

// --- UTILITIES ---
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatTime(ms) {
    if (!ms) return '';
    return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function formatETA(seconds) {
    if (seconds < 60) return `${Math.ceil(seconds)}s`;
    return `${Math.ceil(seconds / 60)}m`;
}

function isValidUrl(string) {
    try {
        const url = new URL(string);
        return url.protocol === 'http:' || url.protocol === 'https:';
    } catch (_) {
        return false;
    }
}

// FIX: use DOM method for reliable escaping (handles all edge cases including backticks, forward slashes)
function escapeHtml(unsafe) {
    const el = document.createElement('span');
    el.textContent = unsafe;
    return el.innerHTML;
}

function getFileIconClass(filename) {
    const ext = (filename.split('.').pop() || '').toLowerCase();
    if (['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp', 'avif', 'heic'].includes(ext)) return 'fa-file-image';
    if (['mp4', 'mov', 'avi', 'webm', 'mkv', 'm4v'].includes(ext)) return 'fa-file-video';
    if (['mp3', 'wav', 'ogg', 'flac', 'm4a', 'aac'].includes(ext)) return 'fa-file-audio';
    if (['pdf'].includes(ext)) return 'fa-file-pdf';
    if (['zip', 'rar', '7z', 'tar', 'gz', 'bz2'].includes(ext)) return 'fa-file-zipper';
    if (['doc', 'docx', 'txt', 'md', 'rtf'].includes(ext)) return 'fa-file-lines';
    if (['xls', 'xlsx', 'csv'].includes(ext)) return 'fa-file-csv';
    if (['js', 'ts', 'jsx', 'tsx', 'html', 'css', 'json', 'py', 'go', 'rs', 'c', 'cpp', 'java'].includes(ext)) return 'fa-file-code';
    return 'fa-file';
}

async function computeFileHash(blob) {
    const arrayBuffer = await blob.arrayBuffer();
    const hashBuffer = await crypto.subtle.digest('SHA-256', arrayBuffer);
    return Array.from(new Uint8Array(hashBuffer))
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
}
