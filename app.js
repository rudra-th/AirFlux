// ============================================
// AirFlux — P2P File & Text Transfer Engine
// ============================================

// --- STATE & CONFIG ---
const CHUNK_SIZE = 512 * 1024;
const PEER_PREFIX = 'passcode-airdrop-v1-';
const MAX_PEER_RETRIES = 10;
const CONNECTION_TIMEOUT_MS = 15000;
const READ_AHEAD = 64;
const BACKPRESSURE_THRESHOLD = 8 * 1024 * 1024;
const BACKPRESSURE_CHECK_MS = 5;
const ACK_INTERVAL = 16;

let peer = null;
let conn = null;
let my4DigitCode = '';
let selectedFiles = [];
let sendingInProgress = false;
let pendingTransfers = {};
let incomingFiles = {};
let fileDomCache = {};
let feedItemCount = 0;
let peerRetryCount = 0;
let connectionTimeout = null;
let connectionAttemptActive = false;

// --- DOM ELEMENTS ---
const $ = (id) => document.getElementById(id);
const statusPill = $('statusPill');
const statusDot = $('statusDot');
const statusText = $('statusText');
const myRoomCodeEl = $('myRoomCode');
const joinCodeInput = $('joinCodeInput');
const feedContainer = $('feedContainer');
const emptyState = $('emptyState');
const feedCountEl = $('feedCount');
const dropzone = $('dropzone');
const dropzonePrompt = $('dropzonePrompt');
const selectedFileState = $('selectedFileState');
const retryOverlay = $('retryOverlay');

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', () => {
    initPeer();
    setupDragAndDrop();
    checkUrlHashForAutoJoin();
});

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
    e.preventDefault();
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

    conn.on('data', (data) => {
        handleIncomingData(data);
    });

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
        handleJoin(new Event('submit', { cancelable: true }));
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

// --- DATA CHANNEL PROTOCOL & CHUNK STREAMING ---
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
            canStreamToDisk: canStreamToDisk,
            diskWritable: null,
            diskHandle: null,
            pendingDiskWrite: Promise.resolve()
        };
        addIncomingFileCard(data.fileId, data.name, data.size);
        playNotificationPulse();
    } else if (data.type === 'file-chunk') {
        const fileObj = incomingFiles[data.fileId];
        if (fileObj) {
            if (fileObj.receivedChunkIndices.has(data.chunkIndex)) return;
            fileObj.receivedChunkIndices.add(data.chunkIndex);
            if (fileObj.diskWritable) {
                const chunkData = data.data;
                fileObj.pendingDiskWrite = fileObj.pendingDiskWrite.then(() => {
                    return fileObj.diskWritable.write(new Uint8Array(chunkData));
                }).catch(() => {});
                fileObj.buffers[data.chunkIndex] = null;
            } else {
                fileObj.buffers[data.chunkIndex] = data.data;
            }
            fileObj.receivedChunks++;
            fileObj.receivedBytes += data.data.byteLength;

            if (fileObj.receivedChunks % ACK_INTERVAL === 0) {
                conn.send({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: data.chunkIndex });
            }

            const now = performance.now();
            if (now - fileObj.lastDomUpdate < 100) return;
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
            updateFileCardProgress(data.fileId, percent, speed);
        }
    } else if (data.type === 'file-end') {
        const fileObj = incomingFiles[data.fileId];
        if (fileObj) {
            if (fileObj.diskWritable) {
                fileObj.pendingDiskWrite.then(async () => {
                    try { await fileObj.diskWritable.close(); } catch (_) {}
                    showDiskSaveComplete(data.fileId);
                });
                fileObj.assembledBlob = null;
                fileObj.downloadUrl = null;
                finishFileCard(data.fileId, null, fileObj.name, fileObj.size, 'pending');
            } else {
                const blob = new Blob(fileObj.buffers, { type: fileObj.fileType });
                const downloadUrl = URL.createObjectURL(blob);
                fileObj.assembledBlob = blob;
                fileObj.downloadUrl = downloadUrl;
                finishFileCard(data.fileId, downloadUrl, fileObj.name, fileObj.size, 'pending');
            }
            showToast(`Received: ${fileObj.name} (verifying integrity...)`, 'info');
            playReceiveSound();
            conn.send({ type: 'file-ack', fileId: data.fileId, lastChunkIndex: fileObj.totalChunks - 1 });
        }
    } else if (data.type === 'file-hash') {
        const fileObj = incomingFiles[data.fileId];
        if (fileObj) {
            if (fileObj.diskWritable) {
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
                    showToast(`Integrity mismatch for ${fileObj.name}! File may be corrupted.`, 'warning');
                }
                delete incomingFiles[data.fileId];
            });
        }
    } else if (data.type === 'file-ack') {
        const pending = pendingTransfers[data.fileId];
        if (pending) {
            pending.lastAckedChunk = Math.max(pending.lastAckedChunk, data.lastChunkIndex);
        }
    } else if (data.type === 'file-resume') {
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
                pendingDiskWrite: Promise.resolve()
            };
            addIncomingFileCard(data.fileId, data.name, data.size);
        }
        const fileObj = incomingFiles[data.fileId];
        fileObj.totalChunks = data.totalChunks;
        const existingCard = $(`file-card-${data.fileId}`);
        if (existingCard) {
            const percent = Math.round((fileObj.receivedChunks / fileObj.totalChunks) * 100);
            updateFileCardProgress(data.fileId, percent, 0);
        }
        showToast(`Resuming: ${data.name} from ${Math.round((fileObj.receivedChunks / fileObj.totalChunks) * 100)}%`, 'info');
        playNotificationPulse();
    }
}

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
            fileId: fileId,
            name: file.name,
            size: file.size,
            fileType: file.type || 'application/octet-stream',
            totalChunks: totalChunks,
            resumeFrom: resumeFrom
        });
    } else {
        conn.send({
            type: 'file-start',
            fileId: fileId,
            name: file.name,
            size: file.size,
            fileType: file.type || 'application/octet-stream',
            totalChunks: totalChunks
        });
    }

    pendingTransfers[fileId] = { file, totalChunks, lastAckedChunk: resumeFrom - 1 };

    if ($(`file-card-${fileId}`)) {
        updateFileCardProgress(fileId, Math.round((resumeFrom / totalChunks) * 100), 0);
    } else {
        addOutgoingFileCard(fileId, file.name, file.size);
    }

    const dc = conn._dc || null;
    const startTime = performance.now();
    let bytesSent = 0;
    let lastSpeedUpdate = startTime;
    let lastBytesAtSpeedUpdate = 0;
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
            conn.send({
                type: 'file-chunk',
                fileId: fileId,
                chunkIndex: chunkIndex,
                data: buffer
            });
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
                speed = ((bytesSent - lastBytesAtSpeedUpdate) / (windowElapsed / 1000));
                lastSpeedUpdate = now;
                lastBytesAtSpeedUpdate = bytesSent;
            } else {
                const elapsed = now - startTime;
                if (elapsed > 0) speed = bytesSent / (elapsed / 1000);
            }
            const percent = Math.round(((chunkIndex + 1) / totalChunks) * 100);
            updateFileCardProgress(fileId, percent, speed);
        }
    }

    delete pendingTransfers[fileId];
    conn.send({ type: 'file-end', fileId: fileId });

    computeFileHash(file).then(hash => {
        conn.send({ type: 'file-hash', fileId: fileId, hash: hash });
    });

    const totalTime = ((performance.now() - startTime) / 1000).toFixed(1);
    const avgSpeed = file.size / ((performance.now() - startTime) / 1000);
    updateFileCardProgress(fileId, 100, avgSpeed);
    finishOutgoingFileCard(fileId, totalTime, avgSpeed);
    showToast(`Sent: ${file.name} (${totalTime}s avg ${formatBytes(Math.round(avgSpeed))}/s)`, 'success');
}

function sendText() {
    const textEl = $('textInput');
    const content = textEl.value.trim();
    if (!content) return;

    if (!conn || !conn.open) {
        showToast('Connect to a peer device before sending text.', 'error');
        return;
    }

    const payload = { type: 'text', content: content, timestamp: Date.now() };
    conn.send(payload);
    addOutgoingTextCard(content, payload.timestamp);
    textEl.value = '';
    showToast('Text sent to peer!', 'success');
}

function handleTextKeydown(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        sendText();
    }
}

// --- UI FEED RENDERING ---
function checkEmptyState() {
    emptyState.classList.toggle('hidden', feedItemCount > 0);
    feedCountEl.textContent = feedItemCount;
}

function addIncomingTextCard(content, timestamp) {
    feedItemCount++;
    checkEmptyState();

    const safeLink = isValidUrl(content);
    const cardId = 'card-' + Math.random().toString(36).substring(2, 9);

    const card = document.createElement('div');
    card.id = cardId;
    card.className = 'feed-card';

    let actionButtonsHtml = `
        <button onclick="copyCardText('${encodeURIComponent(content)}', this)" class="btn-action btn-action-ghost">
            <i class="fa-regular fa-copy"></i> Copy Text
        </button>
    `;

    if (safeLink) {
        actionButtonsHtml = `
            <a href="${escapeHtml(content)}" target="_blank" rel="noopener noreferrer" class="btn-action btn-action-indigo">
                <i class="fa-solid fa-arrow-up-right-from-square"></i> Open Link
            </a>
            <button onclick="copyCardText('${encodeURIComponent(content)}', this)" class="btn-action btn-action-ghost">
                <i class="fa-regular fa-copy"></i> Copy Link
            </button>
        `;
    }

    card.innerHTML = `
        <div class="feed-card-header">
            <span class="badge badge-indigo">
                ${safeLink ? '<i class="fa-solid fa-link"></i> URL Link' : '<i class="fa-regular fa-message"></i> Text Note'}
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
                <i class="fa-solid fa-arrow-right-from-bracket"></i> Sent by You
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

    const card = document.createElement('div');
    card.id = `file-card-${fileId}`;
    card.className = 'file-card';

    const canStreamToDisk = typeof window.showSaveFilePicker === 'function';

    card.innerHTML = `
        <div class="feed-card-header">
            <span class="badge badge-emerald">
                <i class="fa-solid fa-cloud-arrow-down"></i> Incoming Stream
            </span>
            <span class="file-size">${formatBytes(size)}</span>
        </div>
        <div class="file-card-row">
            <div class="file-icon-box emerald">
                <i class="fa-solid ${getFileIconClass(name)}"></i>
            </div>
            <div style="overflow:hidden;flex:1">
                <p class="file-card-name" title="${escapeHtml(name)}">${escapeHtml(name)}</p>
                <p class="file-card-progress-text progress-text">Receiving chunks... 0%</p>
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
            </button>
            ` : ''}
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
            <span class="badge badge-teal">
                <i class="fa-solid fa-cloud-arrow-up"></i> Outgoing Stream
            </span>
            <span class="file-size">${formatBytes(size)}</span>
        </div>
        <div class="file-card-row">
            <div class="file-icon-box teal">
                <i class="fa-solid ${getFileIconClass(name)}"></i>
            </div>
            <div style="overflow:hidden;flex:1">
                <p class="file-card-name">${escapeHtml(name)}</p>
                <p class="file-card-progress-text progress-text">Streaming to peer... 0%</p>
            </div>
        </div>
        <div class="progress-track">
            <div class="progress-bar teal" style="width:0%"></div>
        </div>
    `;

    feedContainer.insertBefore(card, feedContainer.firstChild);
}

function updateFileCardProgress(fileId, percent, speed) {
    const cached = fileDomCache[fileId];
    if (cached) {
        cached.bar.style.width = `${percent}%`;
        const speedStr = speed > 0 ? ` at ${formatBytes(Math.round(speed))}/s` : '';
        cached.text.textContent = `Transferring... ${percent}%${speedStr}`;
        return;
    }
    const card = $(`file-card-${fileId}`);
    if (!card) return;
    const bar = card.querySelector('.progress-bar');
    const text = card.querySelector('.progress-text');
    fileDomCache[fileId] = { bar, text };
    if (bar) bar.style.width = `${percent}%`;
    if (text) {
        const speedStr = speed > 0 ? ` at ${formatBytes(Math.round(speed))}/s` : '';
        text.textContent = `Transferring... ${percent}%${speedStr}`;
    }
}

function finishFileCard(fileId, downloadUrl, name, size, verificationStatus) {
    delete fileDomCache[fileId];
    const card = $(`file-card-${fileId}`);
    if (!card) return;

    const fileObj = incomingFiles[fileId];
    const wasStreamed = fileObj && fileObj.diskWritable;

    const text = card.querySelector('.progress-text');
    if (verificationStatus === 'pending') {
        if (text) text.textContent = 'Transfer complete — Verifying integrity...';
    } else if (verificationStatus === 'verified') {
        if (text) text.textContent = 'Transfer complete — Integrity verified';
    } else if (verificationStatus === 'mismatch') {
        if (text) {
            text.textContent = 'Transfer complete — Integrity MISMATCH (file may be corrupted)';
            text.classList.add('error');
        }
    } else {
        if (text) text.textContent = 'Transfer complete — Ready for download';
    }

    const bar = card.querySelector('.progress-bar');
    if (bar) {
        bar.style.width = '100%';
        bar.classList.replace('emerald', 'emerald-light');
    }

    const previewEl = card.querySelector('.file-preview');
    if (previewEl && !previewEl.hasChildNodes() && downloadUrl && fileObj && fileObj.fileType) {
        addFilePreview(previewEl, fileObj.fileType, downloadUrl);
    }

    const actionArea = card.querySelector('.action-area');
    if (actionArea) {
        actionArea.classList.add('visible');

        let vbClass = 'vb-slate';
        let vbIcon = 'fa-hard-drive';
        let vbText = formatBytes(size);

        if (wasStreamed) {
            vbClass = 'vb-emerald';
            vbIcon = 'fa-hard-drive';
            vbText = 'Saved to Disk';
        } else if (verificationStatus === 'verified') {
            vbClass = 'vb-emerald';
            vbIcon = 'fa-shield-check';
            vbText = 'Verified';
        } else if (verificationStatus === 'mismatch') {
            vbClass = 'vb-red';
            vbIcon = 'fa-triangle-exclamation';
            vbText = 'Unverified';
        } else if (verificationStatus === 'pending') {
            vbClass = 'vb-amber';
            vbIcon = 'fa-spinner';
            vbText = 'Verifying...';
        }

        if (wasStreamed) {
            actionArea.innerHTML = `
                <span class="verification-badge ${vbClass}">
                    <i class="fa-solid ${vbIcon}"></i> ${vbText}
                </span>
            `;
        } else {
            const canStreamToDisk = typeof window.showSaveFilePicker === 'function';

            actionArea.innerHTML = `
                <span class="verification-badge ${vbClass}">
                    <i class="fa-solid ${vbIcon}"></i> ${vbText}
                </span>
                ${canStreamToDisk ? `
                <button onclick="streamToDisk('${fileId}')" class="btn-action btn-action-ghost">
                    <i class="fa-solid fa-hard-drive"></i> Save to Disk
                </button>
                ` : ''}
                <a href="${downloadUrl}" download="${escapeHtml(name)}" class="btn-action btn-action-success">
                    <i class="fa-solid fa-download"></i> Download
                </a>
            `;
        }
    }
}

async function startStreamToDisk(fileId) {
    const fileObj = incomingFiles[fileId];
    if (!fileObj || fileObj.diskWritable) return;

    try {
        const handle = await window.showSaveFilePicker({
            suggestedName: fileObj.name,
            types: [{
                description: 'All Files',
                accept: { '*/*': [] }
            }]
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

        if (fileObj.buffers) {
            const bufferedChunks = [];
            for (let i = 0; i < fileObj.buffers.length; i++) {
                if (fileObj.buffers[i]) bufferedChunks.push({ index: i, data: fileObj.buffers[i] });
            }
            bufferedChunks.sort((a, b) => a.index - b.index);
            fileObj.pendingDiskWrite = fileObj.pendingDiskWrite.then(async () => {
                for (const chunk of bufferedChunks) {
                    await writable.write(new Uint8Array(chunk.data));
                }
            });
            fileObj.buffers = null;
        }

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
    if (fileObj) {
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
            types: [{
                description: 'All Files',
                accept: { '*/*': [] }
            }]
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

function finishOutgoingFileCard(fileId, totalTime, avgSpeed) {
    const card = $(`file-card-${fileId}`);
    if (!card) return;
    const text = card.querySelector('.progress-text');
    if (text) {
        const speedStr = avgSpeed > 0 ? ` avg ${formatBytes(Math.round(avgSpeed))}/s` : '';
        text.textContent = `Delivered to peer in ${totalTime}s${speedStr}`;
    }
}

function clearFeed() {
    feedContainer.innerHTML = '';
    feedContainer.appendChild(emptyState);
    feedItemCount = 0;
    checkEmptyState();
    showToast('Feed cleared', 'info');
}

// --- DRAG & DROP AND FILE SELECT UI ---
function setupDragAndDrop() {
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropzone.addEventListener(eventName, (e) => {
            e.preventDefault();
            e.stopPropagation();
        }, false);
    });

    ['dragenter', 'dragover'].forEach(eventName => {
        dropzone.addEventListener(eventName, () => {
            dropzone.classList.add('dragover');
        }, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropzone.addEventListener(eventName, () => {
            dropzone.classList.remove('dragover');
        }, false);
    });

    dropzone.addEventListener('drop', (e) => {
        const files = e.dataTransfer.files;
        if (files && files.length > 0) {
            addFilesToQueue(Array.from(files));
        }
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
    $('fileInput').value = '';
    selectedFileState.classList.add('hidden');
    dropzonePrompt.classList.remove('hidden');
}

function updateFileListUI() {
    if (selectedFiles.length === 0) {
        selectedFiles = [];
        $('fileInput').value = '';
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
            <button type="button" onclick="removeFile(${i})" class="btn-clear-file">
                <i class="fa-solid fa-xmark"></i>
            </button>
        </div>
    `).join('');

    const totalSize = selectedFiles.reduce((sum, f) => sum + f.size, 0);
    summary.textContent = `${selectedFiles.length} file${selectedFiles.length > 1 ? 's' : ''} \u2014 ${formatBytes(totalSize)}`;
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

// --- QR CODE & AUTO-JOIN ---
function generateQRCode() {
    const qrcodeContainer = $('qrcode');
    qrcodeContainer.innerHTML = '';

    const shareUrl = `${window.location.origin}${window.location.pathname}#join=${my4DigitCode}`;
    $('shareUrlText').textContent = shareUrl;

    new QRCode(qrcodeContainer, {
        text: shareUrl,
        width: 180,
        height: 180,
        colorDark: "#020617",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.M
    });
}

function openQrModal() {
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
            showToast(`Auto-discovered code ${targetCode}. Connecting...`, 'info');
            setTimeout(() => {
                const submitEvent = new Event('submit', { cancelable: true });
                $('joinForm').dispatchEvent(submitEvent);
                window.history.replaceState(null, null, window.location.pathname);
            }, 1200);
        }
    }
}

// --- MICRO-INTERACTIONS & UTILITIES ---
function clearConnectionTimeout() {
    if (connectionTimeout) {
        clearTimeout(connectionTimeout);
        connectionTimeout = null;
    }
}

function copyRoomCode(btn) {
    navigator.clipboard.writeText(my4DigitCode).then(() => {
        triggerCopyFeedback(btn, '<i class="fa-solid fa-check" style="color:var(--emerald-400)"></i>');
        showToast('Room code copied to clipboard!', 'success');
    }).catch(() => {
        showToast('Failed to copy. Check browser permissions.', 'error');
    });
}

function copyShareUrl(btn) {
    const shareUrl = $('shareUrlText').textContent;
    navigator.clipboard.writeText(shareUrl).then(() => {
        triggerCopyFeedback(btn, '<i class="fa-solid fa-check"></i> Copied!');
        showToast('Share link copied to clipboard!', 'success');
    }).catch(() => {
        showToast('Failed to copy. Check browser permissions.', 'error');
    });
}

function copyCardText(encodedText, btn) {
    const decoded = decodeURIComponent(encodedText);
    navigator.clipboard.writeText(decoded).then(() => {
        triggerCopyFeedback(btn, '<i class="fa-solid fa-check" style="color:var(--emerald-400)"></i> Copied!');
    }).catch(() => {
        showToast('Failed to copy. Check browser permissions.', 'error');
    });
}

function triggerCopyFeedback(btnElement, successHtml) {
    const originalHtml = btnElement.innerHTML;
    btnElement.innerHTML = successHtml;
    btnElement.style.borderColor = 'rgba(16, 185, 129, 0.5)';
    setTimeout(() => {
        btnElement.innerHTML = originalHtml;
        btnElement.style.borderColor = '';
    }, 1800);
}

function showToast(message, type = 'info') {
    const container = $('toastContainer');
    const toast = document.createElement('div');

    let bgClass, icon;
    switch (type) {
        case 'success':
            bgClass = 'background:rgba(2,44,34,0.9);border-color:rgba(6,95,70,0.8);color:var(--emerald-950);';
            icon = 'fa-circle-check';
            break;
        case 'error':
            bgClass = 'background:rgba(69,10,10,0.9);border-color:rgba(153,27,27,0.8);color:var(--red-200);';
            icon = 'fa-circle-exclamation';
            break;
        case 'warning':
            bgClass = 'background:rgba(69,26,3,0.9);border-color:rgba(146,64,14,0.8);color:var(--amber-200);';
            icon = 'fa-triangle-exclamation';
            break;
        default:
            bgClass = 'background:var(--slate-900);border-color:var(--slate-800);color:var(--slate-200);';
            icon = 'fa-circle-info';
    }

    const iconColor = {
        success: 'color:var(--emerald-400)',
        error: 'color:var(--red-400)',
        warning: 'color:var(--amber-400)',
        info: 'color:var(--indigo-400)'
    }[type] || 'color:var(--indigo-400)';

    toast.style.cssText = `${bgClass} border:1px solid; box-shadow:0 25px 50px -12px rgba(0,0,0,0.25); border-radius:0.75rem; padding:0.875rem; display:flex; align-items:center; gap:0.75rem; pointer-events:auto; transform:translateY(1rem); opacity:0; transition:all 0.3s ease; font-size:0.75rem; font-weight:500;`;

    toast.innerHTML = `
        <i class="fa-solid ${icon}" style="${iconColor};font-size:1rem;flex-shrink:0"></i>
        <span style="flex:1">${escapeHtml(message)}</span>
    `;

    container.appendChild(toast);
    requestAnimationFrame(() => {
        toast.style.transform = 'translateY(0)';
        toast.style.opacity = '1';
    });

    setTimeout(() => {
        toast.style.transform = 'translateY(0.5rem)';
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 3500);
}

function playNotificationPulse() {
    const header = document.querySelector('header');
    header.style.borderColor = 'rgba(16, 185, 129, 0.8)';
    setTimeout(() => header.style.borderColor = '', 1000);
}

function playReceiveSound() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
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
    } else if (fileType.startsWith('text/') || fileType === 'application/json' || fileType === 'application/javascript') {
        fetch(blobUrl).then(r => r.text()).then(text => {
            const truncated = text.length > 600 ? text.slice(0, 600) + '\n...' : text;
            const pre = document.createElement('pre');
            pre.className = 'preview-text';
            pre.textContent = truncated;
            container.appendChild(pre);
        }).catch(() => {});
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatTime(ms) {
    if (!ms) return '';
    const d = new Date(ms);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function isValidUrl(string) {
    try {
        const url = new URL(string);
        return url.protocol === 'http:' || url.protocol === 'https:';
    } catch (_) {
        return false;
    }
}

function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function getFileIconClass(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    if (['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp'].includes(ext)) return 'fa-file-image';
    if (['mp4', 'mov', 'avi', 'webm', 'mkv'].includes(ext)) return 'fa-file-video';
    if (['mp3', 'wav', 'ogg', 'flac'].includes(ext)) return 'fa-file-audio';
    if (['pdf'].includes(ext)) return 'fa-file-pdf';
    if (['zip', 'rar', '7z', 'tar', 'gz'].includes(ext)) return 'fa-file-zipper';
    if (['doc', 'docx', 'txt', 'md'].includes(ext)) return 'fa-file-lines';
    if (['xls', 'xlsx', 'csv'].includes(ext)) return 'fa-file-csv';
    if (['js', 'html', 'css', 'json', 'py', 'ts'].includes(ext)) return 'fa-file-code';
    return 'fa-file';
}

async function computeFileHash(blob) {
    const arrayBuffer = await blob.arrayBuffer();
    const hashBuffer = await crypto.subtle.digest('SHA-256', arrayBuffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}
