// script.js
// AJShare Client Logic

// Configuration
const CHUNK_SIZE = 16384; // 16KB for maximum compatibility across mobile devices
const BUFFER_THRESHOLD = 65536; // 64KB backpressure threshold
const PING_INTERVAL = 30000; // 30 seconds

// Application State
let roomId = '';
let socket = null;
let myId = '';
let peers = new Map(); // peerId -> { pc, dc, name, deviceType }
let pingIntervalId = null;

// File Transfer State (Sender)
let sendFileState = {
  file: null,
  offset: 0,
  targetPeerId: null,
  startTime: null,
  lastBytesSent: 0,
  lastTime: null,
  activeChannel: null,
  activeReads: 0 // Track concurrent disk reads in flight
};

// File Transfer State (Receiver)
let receiveFileState = {
  fileName: '',
  fileSize: 0,
  receivedSize: 0,
  chunks: [],
  senderPeerId: null,
  startTime: null,
  lastBytesReceived: 0,
  lastTime: null
};

// Generate friendly peer names based on user agent or random list
const ADJECTIVES = ['Sleek', 'Quantum', 'Swift', 'Apex', 'Cyber', 'Neon', 'Cosmic', 'Solar', 'Lunar', 'Alpha', 'Velocity'];
const DEVICES = ['Falcon', 'Panther', 'Phoenix', 'Cheetah', 'Eagle', 'Orca', 'Wolf', 'Lynx', 'Stellar', 'Rover'];

function generateFriendlyName() {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const dev = DEVICES[Math.floor(Math.random() * DEVICES.length)];
  return `${adj} ${dev}`;
}

function getDeviceType() {
  const ua = navigator.userAgent;
  if (/(tablet|ipad|playbook|silk)|(android(?!.*mobi))/i.test(ua)) {
    return 'Tablet';
  }
  if (/Mobile|Android|iP(hone|od)|IEMobile|BlackBerry|Kindle|Silk-Accelerated/i.test(ua)) {
    return 'Phone';
  }
  return 'Laptop';
}

// Register Service Worker for streaming
let serviceWorkerRegistration = null;

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/sw.js')
    .then(reg => {
      serviceWorkerRegistration = reg;
      console.log('Service Worker registered successfully');
    })
    .catch(err => {
      console.warn('Service Worker registration failed:', err);
    });
}

// Initialize Application
document.addEventListener('DOMContentLoaded', () => {
  setupRoom();
  connectSignaling();
  setupUIEventListeners();
});

// Setup room hash
function setupRoom() {
  let hash = window.location.hash;
  if (!hash || hash === '#') {
    // Generate random room id
    roomId = Math.random().toString(36).substring(2, 8);
    window.location.hash = roomId;
  } else {
    roomId = hash.substring(1);
  }
  
  document.getElementById('room-id-display').textContent = roomId;
}

// Connect to signaling server
function connectSignaling() {
  const host = window.location.host;
  
  // Detect if running in a local development environment
  const isDev = !host || 
                host.startsWith('localhost') || 
                host.startsWith('127.0.0.1') || 
                host.startsWith('192.168.') || 
                host.startsWith('10.') || 
                host.startsWith('172.');

  let wsProtocol = 'wss:';
  let wsHost = 'ajshare.mehtaajay8873.workers.dev';

  if (isDev && window.location.protocol !== 'file:') {
    wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    wsHost = host;
  }
  
  const wsUrl = `${wsProtocol}//${wsHost}/ws?room=${roomId}`;
  
  updateConnectionStatus('connecting', 'Connecting...');
  
  socket = new WebSocket(wsUrl);
  socket.binaryType = 'arraybuffer';
  
  socket.addEventListener('open', () => {
    updateConnectionStatus('online', 'Online');
    showToast('Connected to signaling server');
    
    // Start keep-alive pinging
    pingIntervalId = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: 'ping' }));
      }
    }, PING_INTERVAL);
  });
  
  socket.addEventListener('message', async (event) => {
    try {
      if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);
        handleSignalingMessage(msg);
      } else {
        // Relayed binary chunk from the WebSocket signaling server
        handleRelayedWebSocketChunk(event.data);
      }
    } catch (err) {
      console.error('Error parsing signaling message:', err);
    }
  });
  
  socket.addEventListener('close', () => {
    updateConnectionStatus('offline', 'Disconnected');
    showToast('Connection lost. Reconnecting in 5s...', 'info');
    clearInterval(pingIntervalId);
    
    // Cleanup active peers on disconnect
    peers.forEach((peer, peerId) => removePeer(peerId));
    
    setTimeout(connectSignaling, 5000);
  });
}

function handleSignalingMessage(msg) {
  switch (msg.type) {
    case 'welcome':
      myId = msg.clientId;
      document.getElementById('my-peer-id').textContent = `${getDeviceType()} • ID: ${myId}`;
      document.getElementById('my-avatar').textContent = myId.substring(0, 2).toUpperCase();
      
      // Pre-connect to all existing peers in the room
      if (msg.peers && msg.peers.length > 0) {
        msg.peers.forEach(peerId => {
          initiatePeerConnection(peerId);
        });
      }
      break;
      
    case 'peer-joined':
      showToast('New peer entered the room', 'info');
      // Connection will be initiated by the newly joined peer via the 'welcome' packet.
      // The existing peer just waits to receive the incoming offer signal.
      break;
      
    case 'peer-left':
      showToast('A peer left the room', 'info');
      removePeer(msg.peerId);
      break;
      
    case 'signal':
      const sig = msg.signal;
      if (sig.type === 'ws-meta') {
        receiveFileState = {
          fileName: sig.name,
          fileSize: sig.size,
          receivedSize: 0,
          chunks: [],
          senderPeerId: msg.sender,
          startTime: null,
          lastBytesReceived: 0,
          lastTime: null,
          streamId: Math.random().toString(36).substring(2, 15),
          useStream: false,
          useWebSocketRelay: true
        };
        
        const peerInfo = peers.get(msg.sender);
        // Display accept modal
        document.getElementById('incoming-peer-name').textContent = peerInfo ? peerInfo.name : 'Remote Device';
        document.getElementById('incoming-file-name').textContent = sig.name;
        document.getElementById('incoming-file-size').textContent = formatBytes(sig.size);
        document.getElementById('incoming-modal').classList.add('active');
      } else if (sig.type === 'ws-accept') {
        // Callee accepted our WebSocket Relay request!
        startFileTransmissionWebSocket();
      } else if (sig.type === 'ws-decline') {
        showToast('Peer declined the file transfer', 'danger');
        closeTransferModal();
      } else if (sig.type === 'ws-cancel') {
        showToast('Transfer was cancelled by peer', 'danger');
        closeTransferModal();
        receiveFileState.chunks = [];
      } else {
        // Standard WebRTC signals (offer, answer, candidate)
        handleIncomingSignal(msg.sender, sig);
      }
      break;
  }
}

// RTCPeerConnection creation and management
const rtcConfig = {
  iceServers: [
    { urls: 'stun:global.relay.metered.ca:80' },
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    {
      urls: [
        'turn:global.relay.metered.ca:80',
        'turn:global.relay.metered.ca:443',
        'turn:global.relay.metered.ca:443?transport=tcp',
        'turns:global.relay.metered.ca:443'
      ],
      username: '3b5422fb36b2ee058d2be289',
      credential: 'NqAD8YKrV056XGvi'
    }
  ]
};

function initiatePeerConnection(peerId) {
  if (peers.has(peerId)) return;
  
  const pc = new RTCPeerConnection(rtcConfig);
  const friendlyName = generateFriendlyName();
  
  const peerInfo = {
    pc: pc,
    dc: null,
    name: friendlyName,
    deviceType: 'Device',
    candidateQueue: [], // Queue candidates until remote desc is set
    remoteDescSet: false
  };
  peers.set(peerId, peerInfo);
  
  pc.oniceconnectionstatechange = () => {
    console.log(`ICE Connection State with ${peerId}: ${pc.iceConnectionState}`);
  };
  pc.onconnectionstatechange = () => {
    console.log(`Connection State with ${peerId}: ${pc.connectionState}`);
  };
  
  // Create RTCDataChannel
  const dc = pc.createDataChannel('file-transfer', { ordered: true });
  setupDataChannel(peerId, dc);
  peerInfo.dc = dc;
  
  pc.onicecandidate = (event) => {
    if (event.candidate) {
      console.log(`Local ICE candidate gathered: ${event.candidate.candidate}`);
      const candidateInit = {
        candidate: event.candidate.candidate,
        sdpMid: event.candidate.sdpMid,
        sdpMLineIndex: event.candidate.sdpMLineIndex,
        usernameFragment: event.candidate.usernameFragment
      };
      sendSignal(peerId, { type: 'candidate', candidate: candidateInit });
    }
  };
  
  pc.createOffer()
    .then(offer => pc.setLocalDescription(offer))
    .then(() => {
      sendSignal(peerId, { type: 'offer', sdp: pc.localDescription.sdp });
    })
    .catch(err => console.error('Error creating RTCPeerConnection offer:', err));

  addPeerCardToGrid(peerId, friendlyName);
}

function handleIncomingSignal(peerId, signal) {
  let peerInfo = peers.get(peerId);
  
  if (!peerInfo) {
    // Callee side: First time hearing from this peer
    const pc = new RTCPeerConnection(rtcConfig);
    const friendlyName = generateFriendlyName();
    
    peerInfo = {
      pc: pc,
      dc: null,
      name: friendlyName,
      deviceType: 'Device',
      candidateQueue: [], // Queue candidates until remote desc is set
      remoteDescSet: false
    };
    peers.set(peerId, peerInfo);
    
    pc.oniceconnectionstatechange = () => {
      console.log(`ICE Connection State with ${peerId}: ${pc.iceConnectionState}`);
    };
    pc.onconnectionstatechange = () => {
      console.log(`Connection State with ${peerId}: ${pc.connectionState}`);
    };
    
    pc.onicecandidate = (event) => {
      if (event.candidate) {
        console.log(`Local ICE candidate gathered: ${event.candidate.candidate}`);
        const candidateInit = {
          candidate: event.candidate.candidate,
          sdpMid: event.candidate.sdpMid,
          sdpMLineIndex: event.candidate.sdpMLineIndex,
          usernameFragment: event.candidate.usernameFragment
        };
        sendSignal(peerId, { type: 'candidate', candidate: candidateInit });
      }
    };
    
    pc.ondatachannel = (event) => {
      const dc = event.channel;
      setupDataChannel(peerId, dc);
      peerInfo.dc = dc;
    };
    
    addPeerCardToGrid(peerId, friendlyName);
  }
  
  const pc = peerInfo.pc;
  
  if (signal.type === 'offer') {
    pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: signal.sdp }))
      .then(() => pc.createAnswer())
      .then(answer => pc.setLocalDescription(answer))
      .then(() => {
        sendSignal(peerId, { type: 'answer', sdp: pc.localDescription.sdp });
        peerInfo.remoteDescSet = true;
        // Process any queued candidates
        processCandidateQueue(peerInfo);
      })
      .catch(err => console.error('Error handling incoming WebRTC offer:', err));
  } else if (signal.type === 'answer') {
    pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: signal.sdp }))
      .then(() => {
        peerInfo.remoteDescSet = true;
        // Process any queued candidates
        processCandidateQueue(peerInfo);
      })
      .catch(err => console.error('Error setting remote description answer:', err));
  } else if (signal.type === 'candidate') {
    console.log(`Remote ICE candidate received: ${signal.candidate.candidate}`);
    if (peerInfo.remoteDescSet) {
      pc.addIceCandidate(signal.candidate)
        .catch(err => console.error('Error adding ICE candidate:', err));
    } else {
      // Queue candidate
      peerInfo.candidateQueue.push(signal.candidate);
    }
  }
}

function processCandidateQueue(peerInfo) {
  if (peerInfo.candidateQueue && peerInfo.candidateQueue.length > 0) {
    peerInfo.candidateQueue.forEach(candidate => {
      peerInfo.pc.addIceCandidate(candidate)
        .catch(err => console.error('Error adding queued ICE candidate:', err));
    });
    peerInfo.candidateQueue = [];
  }
}

function setupDataChannel(peerId, dc) {
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = BUFFER_THRESHOLD;
  
  dc.onopen = () => {
    const card = document.getElementById(`peer-${peerId}`);
    if (card) {
      card.classList.add('active');
    }
  };
  
  dc.onclose = () => {
    const card = document.getElementById(`peer-${peerId}`);
    if (card) {
      card.classList.remove('active');
    }
  };
  
  dc.onmessage = (event) => {
    handleDataChannelMessage(peerId, event.data);
  };
}

function sendSignal(target, signal) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({
      type: 'signal',
      target: target,
      signal: signal
    }));
  }
}

// Data Channel message parsing and flow controls
function handleDataChannelMessage(peerId, data) {
  // If it's a string, it's signaling/metadata JSON
  if (typeof data === 'string') {
    try {
      const msg = JSON.parse(data);
      const peerInfo = peers.get(peerId);
      
      switch (msg.type) {
        case 'meta':
          // File metadata packet received
          receiveFileState = {
            fileName: msg.name,
            fileSize: msg.size,
            receivedSize: 0,
            chunks: [],
            senderPeerId: peerId,
            startTime: null,
            lastBytesReceived: 0,
            lastTime: null,
            streamId: Math.random().toString(36).substring(2, 15), // Unique ID for Service Worker stream
            useStream: false
          };
          
          // Display accept modal
          document.getElementById('incoming-peer-name').textContent = peerInfo ? peerInfo.name : 'Remote Device';
          document.getElementById('incoming-file-name').textContent = msg.name;
          document.getElementById('incoming-file-size').textContent = formatBytes(msg.size);
          document.getElementById('incoming-modal').classList.add('active');
          break;
          
        case 'accept':
          // Callee accepted our file transfer request!
          startFileTransmission();
          break;
          
        case 'decline':
          showToast('Peer declined the file transfer', 'danger');
          closeTransferModal();
          break;
          
        case 'cancel':
          showToast('Transfer was cancelled by peer', 'danger');
          closeTransferModal();
          // Reset receiver state
          receiveFileState.chunks = [];
          break;
      }
    } catch (err) {
      console.error('Error parsing data channel string message:', err);
    }
  } else {
    // ArrayBuffer - actual file chunk
    processIncomingChunk(peerId, data);
  }
}

// Transmission (Sender) logic with backpressure
function selectAndSendFile(peerId) {
  sendFileState.targetPeerId = peerId;
  document.getElementById('file-input').click();
}

function startFileTransmission() {
  const file = sendFileState.file;
  sendFileState.offset = 0;
  sendFileState.startTime = performance.now();
  sendFileState.lastTime = sendFileState.startTime;
  sendFileState.lastBytesSent = 0;
  sendFileState.activeReads = 0;
  
  const peerInfo = peers.get(sendFileState.targetPeerId);
  const dc = peerInfo ? peerInfo.dc : null;
  
  if (!dc || dc.readyState !== 'open') {
    showToast('Data channel not open', 'danger');
    return;
  }
  
  sendFileState.activeChannel = dc;
  
  // Show progress modal
  document.getElementById('transfer-title').textContent = 'Sending File';
  document.getElementById('transfer-peer-info').textContent = `To ${peerInfo ? peerInfo.name : 'Peer'}`;
  document.getElementById('transfer-modal').classList.add('active');
  
  // Set low threshold event
  dc.onbufferedamountlow = () => {
    sendNextChunks();
  };
  
  sendNextChunks();
}

function sendNextChunks() {
  const file = sendFileState.file;
  const dc = sendFileState.activeChannel;
  
  if (!dc || dc.readyState !== 'open') return;
  
  const MAX_CONCURRENT_READS = 4;
  
  while (sendFileState.offset < file.size && 
         dc.bufferedAmount < BUFFER_THRESHOLD && 
         sendFileState.activeReads < MAX_CONCURRENT_READS) {
           
    sendFileState.activeReads++;
    const currentOffset = sendFileState.offset;
    const slice = file.slice(currentOffset, currentOffset + CHUNK_SIZE);
    sendFileState.offset += slice.size;
    
    const reader = new FileReader();
    reader.onload = (e) => {
      sendFileState.activeReads--;
      
      if (dc.readyState === 'open') {
        try {
          dc.send(e.target.result);
          
          sendFileState.lastBytesSent += slice.size;
          updateProgressUI(sendFileState.lastBytesSent, file.size, sendFileState, true);
          
          if (sendFileState.lastBytesSent >= file.size) {
            showToast('File transfer completed!', 'success');
            setTimeout(closeTransferModal, 1500);
          } else {
            sendNextChunks();
          }
        } catch (err) {
          console.error('DataChannel send error:', err);
          showToast('Failed to send chunk', 'danger');
          cancelActiveTransfer();
        }
      }
    };
    reader.readAsArrayBuffer(slice);
  }
}

// WebSocket Relay Transmission Logic
function startFileTransmissionWebSocket() {
  sendFileState.offset = 0;
  sendFileState.startTime = performance.now();
  sendFileState.lastTime = sendFileState.startTime;
  sendFileState.lastBytesSent = 0;
  sendFileState.activeReads = 0;
  
  const peerInfo = peers.get(sendFileState.targetPeerId);
  
  // Show progress modal
  document.getElementById('transfer-title').textContent = 'Sending File (Relay)';
  document.getElementById('transfer-peer-info').textContent = `To ${peerInfo ? peerInfo.name : 'Peer'}`;
  document.getElementById('transfer-modal').classList.add('active');
  
  sendNextChunksWebSocket();
}

function sendNextChunksWebSocket() {
  const file = sendFileState.file;
  const targetPeerId = sendFileState.targetPeerId;
  
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    showToast('Signaling connection lost', 'danger');
    return;
  }
  
  const WS_CHUNK_SIZE = 131072; // 128KB chunks for WebSocket
  const WS_BUFFER_THRESHOLD = 1048576; // 1MB buffer threshold
  const MAX_CONCURRENT_READS = 4;
  
  while (sendFileState.offset < file.size && 
         socket.bufferedAmount < WS_BUFFER_THRESHOLD && 
         sendFileState.activeReads < MAX_CONCURRENT_READS) {
           
    sendFileState.activeReads++;
    const currentOffset = sendFileState.offset;
    const slice = file.slice(currentOffset, currentOffset + WS_CHUNK_SIZE);
    sendFileState.offset += slice.size;
    
    const reader = new FileReader();
    reader.onload = (e) => {
      sendFileState.activeReads--;
      
      if (socket && socket.readyState === WebSocket.OPEN) {
        try {
          const chunk = e.target.result;
          
          // Construct binary payload: 8 bytes targetPeerId + chunk
          const targetBytes = new TextEncoder().encode(targetPeerId); // 8 bytes
          const payload = new Uint8Array(8 + chunk.byteLength);
          payload.set(targetBytes, 0);
          payload.set(new Uint8Array(chunk), 8);
          
          socket.send(payload.buffer);
          
          sendFileState.lastBytesSent += slice.size;
          updateProgressUI(sendFileState.lastBytesSent, file.size, sendFileState, true);
          
          if (sendFileState.lastBytesSent >= file.size) {
            showToast('File transfer completed!', 'success');
            setTimeout(closeTransferModal, 1500);
          } else {
            sendNextChunksWebSocket();
          }
        } catch (err) {
          console.error('WebSocket send error:', err);
          showToast('Failed to send chunk', 'danger');
          cancelActiveTransfer();
        }
      }
    };
    reader.readAsArrayBuffer(slice);
  }
  
  // If buffer is full, schedule a check shortly
  if (sendFileState.offset < file.size && socket.bufferedAmount >= WS_BUFFER_THRESHOLD) {
    setTimeout(sendNextChunksWebSocket, 5);
  }
}

// WebSocket Relay Reception Logic
function handleRelayedWebSocketChunk(arrayBuffer) {
  if (arrayBuffer.byteLength > 8) {
    const senderBytes = arrayBuffer.slice(0, 8);
    const senderId = new TextDecoder().decode(senderBytes);
    const chunk = arrayBuffer.slice(8);
    
    processIncomingChunk(senderId, chunk);
  }
}

function processIncomingChunk(peerId, data) {
  if (!receiveFileState.startTime) {
    receiveFileState.startTime = performance.now();
    receiveFileState.lastTime = receiveFileState.startTime;
    
    // Show transfer overlay as receiving
    document.getElementById('transfer-title').textContent = 'Receiving File';
    const peer = peers.get(receiveFileState.senderPeerId);
    document.getElementById('transfer-peer-info').textContent = `From ${peer ? peer.name : 'Peer'}`;
    document.getElementById('transfer-modal').classList.add('active');
  }
  
  receiveFileState.receivedSize += data.byteLength;
  
  if (receiveFileState.useStream && navigator.serviceWorker && navigator.serviceWorker.controller) {
    // Feed chunk directly to Service Worker stream
    navigator.serviceWorker.controller.postMessage({
      type: 'WRITE_CHUNK',
      streamId: receiveFileState.streamId,
      chunk: data
    });
  } else {
    // Fallback: Buffer in memory
    receiveFileState.chunks.push(data);
  }
  
  updateProgressUI(receiveFileState.receivedSize, receiveFileState.fileSize, receiveFileState, false);
  
  // Check if fully received
  if (receiveFileState.receivedSize >= receiveFileState.fileSize) {
    finalizeReceivedFile();
  }
}

// Receive completion
function finalizeReceivedFile() {
  showToast('File received successfully!', 'success');
  
  if (receiveFileState.useStream && navigator.serviceWorker && navigator.serviceWorker.controller) {
    // Close the stream in Service Worker
    navigator.serviceWorker.controller.postMessage({
      type: 'CLOSE_STREAM',
      streamId: receiveFileState.streamId
    });
  } else {
    // Fallback: Create Blob and download
    const blob = new Blob(receiveFileState.chunks);
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = receiveFileState.fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    
    setTimeout(() => {
      URL.revokeObjectURL(url);
    }, 1000);
  }
  
  // Cleanup
  setTimeout(() => {
    closeTransferModal();
    receiveFileState.chunks = [];
  }, 1000);
}

// Progress and speed metrics
function updateProgressUI(current, total, state, isSender) {
  const percent = Math.min(Math.round((current / total) * 100), 100);
  
  document.getElementById('progress-bar').style.width = `${percent}%`;
  document.getElementById('progress-percent').textContent = `${percent}%`;
  document.getElementById('transferred-bytes').textContent = `${formatBytes(current)} / ${formatBytes(total)}`;
  
  const now = performance.now();
  const timeElapsed = (now - state.startTime) / 1000; // seconds
  
  if (timeElapsed > 0.5) {
    const bytesTransferred = current;
    const speedBytesPerSec = bytesTransferred / timeElapsed;
    document.getElementById('transfer-speed').textContent = `${formatBytes(speedBytesPerSec)}/s`;
    
    const remainingBytes = total - current;
    if (speedBytesPerSec > 0) {
      const remainingSecs = Math.round(remainingBytes / speedBytesPerSec);
      document.getElementById('time-remaining').textContent = formatTime(remainingSecs);
    } else {
      document.getElementById('time-remaining').textContent = 'Calculating...';
    }
  } else {
    document.getElementById('transfer-speed').textContent = '0 KB/s';
    document.getElementById('time-remaining').textContent = 'Calculating...';
  }
}

// Cancel transfers
function cancelActiveTransfer() {
  // If sender
  if (sendFileState.activeChannel || sendFileState.useWebSocketRelay) {
    try {
      if (sendFileState.useWebSocketRelay) {
        sendSignal(sendFileState.targetPeerId, { type: 'ws-cancel' });
      } else {
        sendFileState.activeChannel.send(JSON.stringify({ type: 'cancel' }));
      }
    } catch(e){}
    sendFileState.activeChannel = null;
    sendFileState.useWebSocketRelay = false;
  }
  
  // If receiver
  if (receiveFileState.senderPeerId) {
    try {
      if (receiveFileState.useWebSocketRelay) {
        sendSignal(receiveFileState.senderPeerId, { type: 'ws-cancel' });
      } else {
        const peer = peers.get(receiveFileState.senderPeerId);
        if (peer && peer.dc && peer.dc.readyState === 'open') {
          peer.dc.send(JSON.stringify({ type: 'cancel' }));
        }
      }
    } catch(e){}
    
    if (receiveFileState.useStream && navigator.serviceWorker && navigator.serviceWorker.controller) {
      navigator.serviceWorker.controller.postMessage({
        type: 'CANCEL_STREAM',
        streamId: receiveFileState.streamId
      });
    }
    
    receiveFileState.chunks = [];
  }
  
  closeTransferModal();
  showToast('Transfer cancelled', 'info');
}

// UI Helper updates & Events
function addPeerCardToGrid(peerId, name) {
  const grid = document.getElementById('peer-grid');
  
  // Remove empty state if present
  const emptyState = document.getElementById('empty-state');
  if (emptyState) {
    emptyState.style.display = 'none';
  }
  
  const card = document.createElement('div');
  card.className = 'peer-card';
  card.id = `peer-${peerId}`;
  
  card.innerHTML = `
    <div class="peer-avatar">${name.substring(0, 2).toUpperCase()}</div>
    <div class="peer-name">${name}</div>
    <div class="peer-desc">Click card to send file</div>
  `;
  
  card.addEventListener('click', () => {
    selectAndSendFile(peerId);
  });
  
  grid.appendChild(card);
  updatePeerCount();
}

function removePeer(peerId) {
  const card = document.getElementById(`peer-${peerId}`);
  if (card) {
    card.remove();
  }
  
  const peerInfo = peers.get(peerId);
  if (peerInfo) {
    if (peerInfo.dc) peerInfo.dc.close();
    if (peerInfo.pc) peerInfo.pc.close();
    peers.delete(peerId);
  }
  
  updatePeerCount();
  
  if (peers.size === 0) {
    const emptyState = document.getElementById('empty-state');
    if (emptyState) {
      emptyState.style.display = 'flex';
    }
  }
}

function updatePeerCount() {
  document.getElementById('peer-count').textContent = `${peers.size} connected`;
}

function updateConnectionStatus(status, text) {
  const indicator = document.getElementById('connection-status');
  indicator.className = `connection-indicator ${status}`;
  indicator.querySelector('.status-text').textContent = text;
}

function setupUIEventListeners() {
  // Copy Room Link
  document.getElementById('copy-room-btn').addEventListener('click', copyRoomLink);
  document.getElementById('invite-btn').addEventListener('click', copyRoomLink);
  
  // File Input Handler
  document.getElementById('file-input').addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file && sendFileState.targetPeerId) {
      sendFileState.file = file;
      
      const peer = peers.get(sendFileState.targetPeerId);
      
      const forceRelay = document.getElementById('relay-toggle').checked;
      
      if (peer && peer.dc && peer.dc.readyState === 'open' && !forceRelay) {
        sendFileState.useWebSocketRelay = false;
        
        // Send meta information to receiver
        peer.dc.send(JSON.stringify({
          type: 'meta',
          name: file.name,
          size: file.size
        }));
      } else {
        // Fallback: Send meta over WebSocket Signaling channel
        sendFileState.useWebSocketRelay = true;
        showToast('Relaying via WebSocket...', 'info');
        
        sendSignal(sendFileState.targetPeerId, {
          type: 'ws-meta',
          name: file.name,
          size: file.size
        });
      }
      
      // Show indicator that we are waiting for user acceptance
      document.getElementById('transfer-title').textContent = 'Waiting for Accept';
      document.getElementById('transfer-peer-info').textContent = `Waiting for ${peer ? peer.name : 'Peer'} to accept...`;
      document.getElementById('transfer-speed').textContent = '-';
      document.getElementById('time-remaining').textContent = '-';
      document.getElementById('progress-bar').style.width = '0%';
      document.getElementById('progress-percent').textContent = '0%';
      document.getElementById('transferred-bytes').textContent = `File: ${file.name} (${formatBytes(file.size)})`;
      document.getElementById('transfer-modal').classList.add('active');
    }
  });
  
  // Accept / Decline Modals
  document.getElementById('accept-file-btn').addEventListener('click', async () => {
    document.getElementById('incoming-modal').classList.remove('active');
    
    // Check if Service Worker is active and ready
    const sw = navigator.serviceWorker && navigator.serviceWorker.controller;
    
    if (sw) {
      // 1. Tell Service Worker to create the response stream
      const messageChannel = new MessageChannel();
      sw.postMessage({
        type: 'CREATE_STREAM',
        streamId: receiveFileState.streamId,
        name: receiveFileState.fileName,
        size: receiveFileState.fileSize
      }, [messageChannel.port2]);

      // Wait for Service Worker OK response
      await new Promise((resolve) => {
        messageChannel.port1.onmessage = (e) => resolve(e.data);
      });

      // 2. Start browser download by loading stream in an iframe
      let iframe = document.getElementById('sw-download-iframe');
      if (!iframe) {
        iframe = document.createElement('iframe');
        iframe.id = 'sw-download-iframe';
        iframe.style.display = 'none';
        document.body.appendChild(iframe);
      }
      iframe.src = `/download-stream/${receiveFileState.streamId}`;
      receiveFileState.useStream = true;
    } else {
      receiveFileState.useStream = false;
      console.warn('Service Worker not active, falling back to in-memory buffering');
    }
    
    // Notify sender that we accepted
    if (receiveFileState.useWebSocketRelay) {
      sendSignal(receiveFileState.senderPeerId, { type: 'ws-accept' });
    } else {
      const peer = peers.get(receiveFileState.senderPeerId);
      if (peer && peer.dc && peer.dc.readyState === 'open') {
        peer.dc.send(JSON.stringify({ type: 'accept' }));
      }
    }
  });
  
  document.getElementById('decline-file-btn').addEventListener('click', () => {
    document.getElementById('incoming-modal').classList.remove('active');
    
    // Notify sender that we declined
    if (receiveFileState.useWebSocketRelay) {
      sendSignal(receiveFileState.senderPeerId, { type: 'ws-decline' });
    } else {
      const peer = peers.get(receiveFileState.senderPeerId);
      if (peer && peer.dc && peer.dc.readyState === 'open') {
        peer.dc.send(JSON.stringify({ type: 'decline' }));
      }
    }
  });
  
  // Cancel active transfers
  document.getElementById('cancel-transfer-btn').addEventListener('click', cancelActiveTransfer);
}

function copyRoomLink() {
  const url = window.location.href;
  navigator.clipboard.writeText(url).then(() => {
    showToast('Room link copied to clipboard!');
  }).catch(err => {
    console.error('Failed to copy text: ', err);
  });
}

function closeTransferModal() {
  document.getElementById('transfer-modal').classList.remove('active');
  // Reset input field so same file can be shared again if needed
  document.getElementById('file-input').value = '';
}

// Formatting helpers
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function formatTime(seconds) {
  if (seconds === Infinity || isNaN(seconds)) return 'Calculating...';
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}m ${secs}s`;
}

// Simple toast feedback
function showToast(message, type = 'success') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  
  let icon = '';
  if (type === 'success') {
    icon = '<svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  } else if (type === 'info') {
    icon = '<svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>';
  } else {
    icon = '<svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>';
  }
  
  toast.innerHTML = `${icon} <span>${message}</span>`;
  container.appendChild(toast);
  
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}
