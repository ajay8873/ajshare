// script.js
// AJShare Client Logic (Multi-Page Version)

// Configuration
const CHUNK_SIZE = 65536; // 64KB chunks for optimal WebRTC DataChannel speed and compatibility
const BUFFER_THRESHOLD = 1048576; // 1MB buffer to optimize throughput speed without overflowing browser SCTP stack
const PING_INTERVAL = 10000; // 10 seconds — keeps signaling alive even during file picker pauses

// Determine current page context
const path = window.location.pathname;
const isIndexPage = path.endsWith('index.html') || path.endsWith('/') || (!path.includes('.html') && !path.includes('room') && !path.includes('peers'));
const isRoomPage = path.includes('room.html');
const isPeersPage = path.includes('peers.html');

// Application State
let roomId = '';
let socket = null;
let myId = '';
let peers = new Map(); // peerId -> { pc, dc, name, deviceType, isCapacitor, localIp }
let pingIntervalId = null;
let qrMode = 'local'; // 'local' or 'internet'
let reconnectTimeoutId = null;
let localIpAddress = '';
let pendingSignals = [];

// App & OS Environment
const isCapacitor = !!window.Capacitor || (window.location.hostname === 'localhost' && !window.location.port);
const isNanoHTTPD = !isCapacitor && window.location.port === '8080' && window.location.hostname !== 'localhost';

// Generate/Load friendly peer names and device IDs
const ADJECTIVES = ['Sleek', 'Quantum', 'Swift', 'Apex', 'Cyber', 'Neon', 'Cosmic', 'Solar', 'Lunar', 'Alpha', 'Velocity'];
const DEVICES = ['Falcon', 'Panther', 'Phoenix', 'Cheetah', 'Eagle', 'Orca', 'Wolf', 'Lynx', 'Stellar', 'Rover'];

function generateFriendlyName() {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const dev = DEVICES[Math.floor(Math.random() * DEVICES.length)];
  return `${adj} ${dev}`;
}

let myFriendlyName = localStorage.getItem('ajshare_friendly_name') || generateFriendlyName();
localStorage.setItem('ajshare_friendly_name', myFriendlyName);

// Scanner controller variable
let html5QrcodeScanner = null;

// Room ID setup
function setupRoomId() {
  let hash = window.location.hash;
  if (hash && hash !== '#' && hash.length > 1) {
    roomId = hash.substring(1).toLowerCase();
  } else {
    roomId = localStorage.getItem('ajshare_room_id') || Math.random().toString(36).substring(2, 8);
    localStorage.setItem('ajshare_room_id', roomId);
    window.location.hash = roomId;
  }
  const display = document.getElementById('room-id-display');
  if (display) {
    display.textContent = roomId;
  }
}

// Same Network Verification
function checkSameNetwork(peerInfo) {
  if (!isCapacitor || !peerInfo.isCapacitor) return;
  if (!localIpAddress || !peerInfo.localIp) return;

  const myOctets = localIpAddress.split('.').slice(0, 3).join('.');
  const peerOctets = peerInfo.localIp.split('.').slice(0, 3).join('.');

  if (myOctets !== peerOctets) {
    console.warn(`Subnet mismatch: ${localIpAddress} vs ${peerInfo.localIp}`);
    const modal = document.getElementById('network-alert-modal');
    if (modal) {
      modal.classList.add('active');
    }
    // Block cards
    const cards = document.querySelectorAll('.peer-card');
    cards.forEach(card => {
      card.style.opacity = '0.5';
      card.style.pointerEvents = 'none';
      const desc = card.querySelector('.peer-desc');
      if (desc) desc.textContent = "Unavailable (Different Network)";
    });
  }
}

// Connection Mode Badge Update Helper
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

// Transfer controls & state
let transferGeneration = 0;
let isSendingChunks = false;
let isSendingWebSocket = false;

let sendFileState = {
  file: null,
  offset: 0,
  targetPeerId: null,
  startTime: null,
  lastBytesSent: 0,
  lastTime: null,
  activeChannel: null,
  activeReads: 0,
  readQueue: new Map(),
  readIndex: 0,
  sendIndex: 0,
  useWebSocketRelay: false,
  isPickingFile: false
};

let receiveFileState = {
  fileName: '',
  fileSize: 0,
  receivedSize: 0,
  chunks: [],
  senderPeerId: null,
  startTime: null,
  lastBytesReceived: 0,
  lastTime: null,
  finalized: false
};

// Connect to signaling server
function connectSignaling() {
  if (!isRoomPage && !isPeersPage) return;

  if (reconnectTimeoutId) {
    clearTimeout(reconnectTimeoutId);
    reconnectTimeoutId = null;
  }
  const host = window.location.host;
  const isDev = (!host || host.startsWith('localhost') || host.startsWith('127.0.0.1')) && !window.Capacitor;

  let wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  let wsHost = 'ajshare.mehtaajay8873.workers.dev';

  if (isDev && window.location.protocol !== 'file:') {
    wsHost = host;
  } else if (window.location.port === '8080' && window.location.hostname !== 'localhost') {
    wsHost = window.location.hostname + ':8081';
  }
  
  if (wsHost === 'ajshare.mehtaajay8873.workers.dev') {
    wsProtocol = 'wss:';
  }
  
  const wsUrl = `${wsProtocol}//${wsHost}/ws?room=${roomId}`;
  updateConnectionStatus('connecting', 'Connecting...');
  
  socket = new WebSocket(wsUrl);
  socket.binaryType = 'arraybuffer';
  
  socket.addEventListener('open', () => {
    updateConnectionStatus('online', 'Online');
    showToast('Connected to signaling server');
    
    pingIntervalId = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: 'ping' }));
      }
    }, PING_INTERVAL);

    while (pendingSignals.length > 0) {
      const pending = pendingSignals.shift();
      sendSignal(pending.target, pending.signal);
    }
  });
  
  socket.addEventListener('message', async (event) => {
    try {
      if (typeof event.data === 'string') {
        const msg = JSON.parse(event.data);
        handleSignalingMessage(msg);
      } else {
        handleRelayedWebSocketChunk(event.data);
      }
    } catch (err) {
      console.error('Error parsing signaling message:', err);
    }
  });
  
  socket.addEventListener('close', () => {
    updateConnectionStatus('offline', 'Disconnected');
    if (!sendFileState.isPickingFile) {
      showToast('Connection lost. Reconnecting...', 'info');
    }
    clearInterval(pingIntervalId);
    
    if (!reconnectTimeoutId) {
      const delay = sendFileState.isPickingFile ? 0 : 5000;
      reconnectTimeoutId = setTimeout(connectSignaling, delay);
    }
  });
}

function handleSignalingMessage(msg) {
  switch (msg.type) {
    case 'welcome':
      myId = msg.clientId;
      const peerIdEl = document.getElementById('my-peer-id');
      if (peerIdEl) {
        peerIdEl.textContent = `${getDeviceType()} • ID: ${myId}`;
      }
      const avatarEl = document.getElementById('my-avatar');
      if (avatarEl) {
        avatarEl.textContent = myFriendlyName.substring(0, 2).toUpperCase();
      }
      
      // If we are on peers page, establish WebRTC with all current room members
      if (isPeersPage && msg.peers && msg.peers.length > 0) {
        msg.peers.forEach(peerId => {
          initiatePeerConnection(peerId);
          sendSignal(peerId, {
            type: 'peer-meta',
            name: myFriendlyName,
            isCapacitor: isCapacitor,
            deviceType: getDeviceType(),
            localIp: localIpAddress
          });
        });
      } else if (isRoomPage && msg.peers && msg.peers.length > 0) {
        // B joined Room Page but room has members. Redirect B directly to peers.html
        window.location.href = `peers.html#${roomId}`;
      }
      break;
      
    case 'peer-joined':
      // Show scan feedback immediately on Room Page
      if (isRoomPage) {
        const waitingTitle = document.getElementById('connection-waiting-title');
        const waitingDesc = document.getElementById('connection-waiting-desc');
        const waitingIcon = document.getElementById('connection-waiting-icon');
        if (waitingTitle) {
          waitingTitle.textContent = "Connecting to peer...";
          waitingDesc.textContent = "Peer scanned QR code or joined room. Establishing secure WebRTC peer connection...";
          waitingIcon.innerHTML = `<div class="avatar-ring pulsing"><div class="avatar" style="background: var(--primary);">Connecting</div></div>`;
        }
        showToast('New peer entered the room, redirecting...', 'info');
        
        // Send meta and redirect to peers page
        sendSignal(msg.peerId, {
          type: 'peer-meta',
          name: myFriendlyName,
          isCapacitor: isCapacitor,
          deviceType: getDeviceType(),
          localIp: localIpAddress
        });
        
        setTimeout(() => {
          window.location.href = `peers.html#${roomId}`;
        }, 800);
      } else if (isPeersPage) {
        if (!peers.has(msg.peerId)) {
          showToast('New peer entered the room', 'info');
        }
        sendSignal(msg.peerId, {
          type: 'peer-meta',
          name: myFriendlyName,
          isCapacitor: isCapacitor,
          deviceType: getDeviceType(),
          localIp: localIpAddress
        });
      }
      break;
      
    case 'peer-left':
      if (isPeersPage) {
        const peer = peers.get(msg.peerId);
        if (peer && (peer.isSelectingFile || (peer.dc && peer.dc.readyState === 'open'))) {
          console.log(`Peer ${msg.peerId} left signaling but WebRTC is active.`);
        } else {
          showToast('A peer left the room', 'info');
          removePeer(msg.peerId);
        }
      }
      break;
      
    case 'signal':
      const sig = msg.signal;
      if (sig.type === 'peer-meta') {
        let peerInfo = peers.get(msg.sender);
        if (!peerInfo) {
          peerInfo = {
            pc: null,
            dc: null,
            name: sig.name,
            deviceType: sig.deviceType,
            isCapacitor: sig.isCapacitor,
            localIp: sig.localIp,
            candidateQueue: [],
            remoteDescSet: false,
            webrtcFailed: false
          };
          peers.set(msg.sender, peerInfo);
        } else {
          peerInfo.name = sig.name;
          peerInfo.deviceType = sig.deviceType;
          peerInfo.isCapacitor = sig.isCapacitor;
          peerInfo.localIp = sig.localIp;
        }

        // Apply same network check
        if (isCapacitor && peerInfo.isCapacitor) {
          checkSameNetwork(peerInfo);
        }

        // Update UI name card
        const card = document.getElementById(`peer-${msg.sender}`);
        if (card) {
          const nameEl = card.querySelector('.peer-name');
          if (nameEl) nameEl.textContent = sig.name;
          const avatarEl = card.querySelector('.peer-avatar');
          if (avatarEl) avatarEl.textContent = sig.name.substring(0, 2).toUpperCase();
        }
      } else if (sig.type === 'direct-download') {
        // Enforce same-network blocking
        const pInfo = peers.get(msg.sender);
        if (isCapacitor && pInfo && pInfo.isCapacitor && localIpAddress && pInfo.localIp) {
          if (localIpAddress.split('.').slice(0, 3).join('.') !== pInfo.localIp.split('.').slice(0, 3).join('.')) {
            showToast('Download blocked: Devices are not on the same local subnet.', 'danger');
            return;
          }
        }

        receiveFileState = {
          fileName: sig.name,
          fileSize: sig.size,
          downloadUrl: sig.downloadUrl,
          senderPeerId: msg.sender,
          isDirectDownload: true
        };
        document.getElementById('incoming-peer-name').textContent = pInfo ? pInfo.name : 'Remote Device';
        document.getElementById('incoming-file-name').textContent = sig.name;
        document.getElementById('incoming-file-size').textContent = formatBytes(sig.size);
        document.getElementById('incoming-modal').classList.add('active');
      } else if (sig.type === 'ws-meta') {
        // Block WebSockets if both are app users
        const pInfo = peers.get(msg.sender);
        if (isCapacitor && pInfo && pInfo.isCapacitor) {
          showToast('WebSocket relay blocked for App-to-App. Please use local Wi-Fi/Hotspot.', 'danger');
          sendSignal(msg.sender, { type: 'ws-decline' });
          return;
        }

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
        document.getElementById('incoming-peer-name').textContent = pInfo ? pInfo.name : 'Remote Device';
        document.getElementById('incoming-file-name').textContent = sig.name;
        document.getElementById('incoming-file-size').textContent = formatBytes(sig.size);
        document.getElementById('incoming-modal').classList.add('active');
      } else if (sig.type === 'ws-accept') {
        startFileTransmissionWebSocket();
      } else if (sig.type === 'ws-decline') {
        showToast('Peer declined the file transfer', 'danger');
        closeTransferModal();
      } else if (sig.type === 'ws-cancel') {
        showToast('Transfer was cancelled by peer', 'danger');
        resetAllTransferStates();
        closeTransferModal();
      } else {
        handleIncomingSignal(msg.sender, sig);
      }
      break;
  }
}

// RTCPeerConnection Config
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
  const existingPeer = peers.get(peerId);
  if (existingPeer) {
    if (existingPeer.dc && (existingPeer.dc.readyState === 'open' || existingPeer.dc.readyState === 'connecting')) return;
    closePeerConnection(peerId);
  }
  
  const pc = new RTCPeerConnection(rtcConfig);
  const friendlyName = existingPeer ? existingPeer.name : generateFriendlyName();
  
  const peerInfo = {
    pc: pc,
    dc: null,
    name: friendlyName,
    deviceType: existingPeer ? existingPeer.deviceType : 'Device',
    localIp: existingPeer ? existingPeer.localIp : '',
    candidateQueue: [],
    remoteDescSet: false,
    webrtcFailed: false
  };
  peers.set(peerId, peerInfo);
  
  pc.oniceconnectionstatechange = () => {
    if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed') {
      handleWebRTCFailure(peerId);
    }
  };
  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      handleWebRTCFailure(peerId);
    }
  };
  
  const dc = pc.createDataChannel('file-transfer', { ordered: true });
  setupDataChannel(peerId, dc);
  peerInfo.dc = dc;
  
  pc.onicecandidate = (event) => {
    if (event.candidate) {
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
  if (peerInfo) {
    peerInfo.webrtcFailed = false;
  }
  
  if (!peerInfo) {
    const pc = new RTCPeerConnection(rtcConfig);
    const friendlyName = generateFriendlyName();
    
    peerInfo = {
      pc: pc,
      dc: null,
      name: friendlyName,
      deviceType: 'Device',
      localIp: '',
      candidateQueue: [],
      remoteDescSet: false,
      webrtcFailed: false
    };
    peers.set(peerId, peerInfo);
    
    pc.oniceconnectionstatechange = () => {
      if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed') {
        handleWebRTCFailure(peerId);
      }
    };
    
    pc.onicecandidate = (event) => {
      if (event.candidate) {
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
        processCandidateQueue(peerInfo);
      })
      .catch(err => console.error('Error handling incoming WebRTC offer:', err));
  } else if (signal.type === 'answer') {
    pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: signal.sdp }))
      .then(() => {
        peerInfo.remoteDescSet = true;
        processCandidateQueue(peerInfo);
      })
      .catch(err => console.error('Error setting remote description answer:', err));
  } else if (signal.type === 'candidate') {
    const parts = signal.candidate.candidate.split(' ');
    if (parts.length >= 8 && parts[7] === 'host') {
      const remoteIp = parts[4];
      if (remoteIp && !remoteIp.endsWith('.local') && !localIpAddress && window.location.protocol !== 'https:') {
        fetch(`http://${remoteIp}:8080/api/peer-ip`)
          .then(res => res.json())
          .then(data => {
            if (data && data.ip) {
              localIpAddress = data.ip;
            }
          })
          .catch(err => console.warn('Failed to query remote local server for peer-ip:', err));
      }
    }

    if (peerInfo.remoteDescSet) {
      pc.addIceCandidate(new RTCIceCandidate(signal.candidate))
        .catch(err => console.error('Error adding ICE candidate:', err));
    } else {
      peerInfo.candidateQueue.push(signal.candidate);
    }
  }
}

function processCandidateQueue(peerInfo) {
  if (peerInfo.candidateQueue && peerInfo.candidateQueue.length > 0) {
    peerInfo.candidateQueue.forEach(candidate => {
      peerInfo.pc.addIceCandidate(new RTCIceCandidate(candidate))
        .catch(err => console.error('Error adding queued ICE candidate:', err));
    });
    peerInfo.candidateQueue = [];
  }
}

function setupDataChannel(peerId, dc) {
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = 262144;
  
  dc.onopen = () => {
    const card = document.getElementById(`peer-${peerId}`);
    if (card) {
      card.classList.add('active');
    }
    updatePeerConnectionBadge(peerId);
  };
  
  dc.onclose = () => {
    handleWebRTCFailure(peerId);
  };
  
  dc.onmessage = (event) => {
    handleDataChannelMessage(peerId, event.data);
  };
}

function rewriteSdpOrCandidate(signal) {
  if (!localIpAddress) return signal;
  
  const signalCopy = JSON.parse(JSON.stringify(signal));
  const injectLocalHostCandidate = (candStr) => {
    if (!localIpAddress) return null;
    const parts = candStr.split(' ');
    if (parts.length < 8) return null;

    const typ = parts[7];
    const ip  = parts[4];

    if (typ === 'host' && ip) {
      parts[4] = localIpAddress;
      return parts.join(' ');
    }
    if (typ === 'srflx') {
      let rport = null;
      for (let i = 8; i < parts.length - 1; i++) {
        if (parts[i] === 'rport' && parts[i + 1] !== '0') {
          rport = parts[i + 1];
          break;
        }
      }
      if (!rport) return null;
      const foundation = parts[0].includes(':') ? parts[0].split(':')[1] : parts[0];
      return `candidate:${foundation} ${parts[1]} udp 2122260223 ${localIpAddress} ${rport} typ host`;
    }
    return null;
  };

  if (signalCopy.type === 'candidate' && signalCopy.candidate) {
    let candStr = signalCopy.candidate.candidate;
    const newCand = injectLocalHostCandidate(candStr);
    if (newCand) {
      signalCopy.candidate.candidate = newCand;
    }
  } else if ((signalCopy.type === 'offer' || signalCopy.type === 'answer') && signalCopy.sdp) {
    const lines = signalCopy.sdp.split('\r\n');
    const newLines = [];
    for (let line of lines) {
      newLines.push(line);
      if (line.startsWith('a=candidate:')) {
        const candStr = line.substring(2);
        const newCand = injectLocalHostCandidate(candStr);
        if (newCand) {
          const newCandLine = 'a=' + newCand;
          if (!newLines.includes(newCandLine)) {
            newLines.push(newCandLine);
          }
        }
      }
    }
    signalCopy.sdp = newLines.join('\r\n');
  }
  return signalCopy;
}

function sendSignal(target, signal) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    const preparedSignal = rewriteSdpOrCandidate(signal);
    socket.send(JSON.stringify({
      type: 'signal',
      target: target,
      signal: preparedSignal
    }));
  } else {
    pendingSignals.push({ target: target, signal: signal });
  }
}

function handleDataChannelMessage(peerId, data) {
  if (typeof data === 'string') {
    try {
      const msg = JSON.parse(data);
      const peerInfo = peers.get(peerId);
      
      switch (msg.type) {
        case 'meta':
          receiveFileState = {
            fileName: msg.name,
            fileSize: msg.size,
            receivedSize: 0,
            chunks: [],
            senderPeerId: peerId,
            startTime: null,
            lastBytesReceived: 0,
            lastTime: null,
            streamId: Math.random().toString(36).substring(2, 15),
            useStream: false
          };
          document.getElementById('incoming-peer-name').textContent = peerInfo ? peerInfo.name : 'Remote Device';
          document.getElementById('incoming-file-name').textContent = msg.name;
          document.getElementById('incoming-file-size').textContent = formatBytes(msg.size);
          document.getElementById('incoming-modal').classList.add('active');
          break;
          
        case 'accept':
          startFileTransmission();
          break;
          
        case 'decline':
          showToast('Peer declined the file transfer', 'danger');
          closeTransferModal();
          break;
          
        case 'selecting-file':
          if (peerInfo) {
            peerInfo.isSelectingFile = true;
            if (peerInfo.selectingFileTimeoutId) clearTimeout(peerInfo.selectingFileTimeoutId);
            peerInfo.selectingFileTimeoutId = setTimeout(() => {
              peerInfo.isSelectingFile = false;
            }, 30000);
          }
          break;
          
        case 'cancel':
          showToast('Transfer was cancelled by peer', 'danger');
          resetAllTransferStates();
          closeTransferModal();
          break;
      }
    } catch (err) {
      console.error('Error parsing data channel string:', err);
    }
  } else {
    processIncomingChunk(peerId, data);
  }
}

function selectAndSendFile(peerId) {
  // Same network check before picking
  const peer = peers.get(peerId);
  if (isCapacitor && peer && peer.isCapacitor) {
    const myOctets = localIpAddress.split('.').slice(0, 3).join('.');
    const peerOctets = peer.localIp.split('.').slice(0, 3).join('.');
    if (myOctets !== peerOctets) {
      const modal = document.getElementById('network-alert-modal');
      if (modal) modal.classList.add('active');
      return;
    }
  }

  sendFileState.targetPeerId = peerId;
  sendFileState.isPickingFile = true;
  if (peer && peer.dc && peer.dc.readyState === 'open') {
    try {
      peer.dc.send(JSON.stringify({ type: 'selecting-file' }));
    } catch(e){}
  }
  document.getElementById('file-input').click();
}

function startFileTransmission() {
  sendFileState.offset = 0;
  sendFileState.startTime = performance.now();
  sendFileState.lastTime = sendFileState.startTime;
  sendFileState.lastBytesSent = 0;
  sendFileState.activeReads = 0;
  sendFileState.readQueue = new Map();
  sendFileState.readIndex = 0;
  sendFileState.sendIndex = 0;
  
  const peerInfo = peers.get(sendFileState.targetPeerId);
  const dc = peerInfo ? peerInfo.dc : null;
  
  if (!dc || dc.readyState !== 'open') {
    showToast('Data channel not open', 'danger');
    return;
  }
  
  sendFileState.activeChannel = dc;
  document.getElementById('transfer-title').textContent = 'Sending File';
  document.getElementById('transfer-peer-info').textContent = `To ${peerInfo ? peerInfo.name : 'Peer'}`;
  
  const badge = document.getElementById('connection-mode-badge');
  badge.textContent = 'Direct P2P (0 Internet Data)';
  badge.className = 'connection-mode-badge direct';
  document.getElementById('transfer-modal').classList.add('active');
  
  dc.onbufferedamountlow = () => {
    sendOrderedChunks();
  };
  
  sendNextChunks();
}

function sendNextChunks() {
  const file = sendFileState.file;
  const dc = sendFileState.activeChannel;
  if (!dc || dc.readyState !== 'open') return;
  
  const MAX_CONCURRENT_READS = 64;
  
  if (dc.bufferedAmount >= BUFFER_THRESHOLD || 
      (sendFileState.readIndex - sendFileState.sendIndex) >= MAX_CONCURRENT_READS) {
    return;
  }
  
  while (sendFileState.offset < file.size && 
         sendFileState.activeReads < MAX_CONCURRENT_READS &&
         (sendFileState.readIndex - sendFileState.sendIndex) < MAX_CONCURRENT_READS) {
           
    sendFileState.activeReads++;
    const currentIndex = sendFileState.readIndex++;
    const currentOffset = sendFileState.offset;
    const slice = file.slice(currentOffset, currentOffset + CHUNK_SIZE);
    sendFileState.offset += slice.size;
    
    slice.arrayBuffer().then((buffer) => {
      sendFileState.activeReads--;
      sendFileState.readQueue.set(currentIndex, buffer);
      sendOrderedChunks();
    }).catch((err) => {
      console.error('File read error:', err);
      sendFileState.activeReads--;
      cancelActiveTransfer();
    });
  }
}

function sendOrderedChunks() {
  if (isSendingChunks) return;
  isSendingChunks = true;

  try {
    const dc = sendFileState.activeChannel;
    if (!dc || dc.readyState !== 'open') return;
    
    while (sendFileState.readQueue.has(sendFileState.sendIndex)) {
      if (dc.bufferedAmount >= BUFFER_THRESHOLD) return;
      
      const index = sendFileState.sendIndex;
      const chunk = sendFileState.readQueue.get(index);
      sendFileState.readQueue.delete(index);
      
      try {
        dc.send(chunk);
        sendFileState.sendIndex++;
        sendFileState.lastBytesSent += chunk.byteLength;
        
        updateProgressUI(sendFileState.lastBytesSent, sendFileState.file.size, sendFileState, true);
        
        if (sendFileState.lastBytesSent >= sendFileState.file.size) {
          showToast('File transfer completed!', 'success');
          addHistoryItem(sendFileState.file.name, sendFileState.file.size, 'sent', 'completed');
          setTimeout(() => {
            resetAllTransferStates();
            closeTransferModal();
          }, 1500);
          return;
        }
      } catch (err) {
        if (err.name === 'InvalidStateError' || err.code === 11 || err.message.toLowerCase().includes('buffer')) {
          sendFileState.readQueue.set(index, chunk);
          return;
        }
        cancelActiveTransfer();
        return;
      }
    }
    sendNextChunks();
  } finally {
    isSendingChunks = false;
  }
}

// WebSocket Relay Transmission
function startFileTransmissionWebSocket() {
  const file = sendFileState.file;
  sendFileState.offset = 0;
  sendFileState.startTime = performance.now();
  sendFileState.lastTime = sendFileState.startTime;
  sendFileState.lastBytesSent = 0;
  sendFileState.activeReads = 0;
  sendFileState.readQueue = new Map();
  sendFileState.readIndex = 0;
  sendFileState.sendIndex = 0;
  sendFileState.targetBytes = new TextEncoder().encode(sendFileState.targetPeerId);
  
  const peerInfo = peers.get(sendFileState.targetPeerId);
  document.getElementById('transfer-title').textContent = 'Sending File (Relay)';
  document.getElementById('transfer-peer-info').textContent = `To ${peerInfo ? peerInfo.name : 'Peer'}`;
  
  const badge = document.getElementById('connection-mode-badge');
  badge.textContent = 'WebSocket Relay (Uses Internet Data)';
  badge.className = 'connection-mode-badge relay';
  document.getElementById('transfer-modal').classList.add('active');
  
  sendNextChunksWebSocket();
}

function sendNextChunksWebSocket() {
  const file = sendFileState.file;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    showToast('Signaling connection lost', 'danger');
    return;
  }
  
  const WS_CHUNK_SIZE = 262144;
  const MAX_CONCURRENT_READS = 16;
  
  while (sendFileState.offset < file.size && 
         sendFileState.activeReads < MAX_CONCURRENT_READS &&
         (sendFileState.readIndex - sendFileState.sendIndex) < MAX_CONCURRENT_READS) {
           
    sendFileState.activeReads++;
    const currentIndex = sendFileState.readIndex++;
    const currentOffset = sendFileState.offset;
    const slice = file.slice(currentOffset, currentOffset + WS_CHUNK_SIZE);
    sendFileState.offset += slice.size;
    
    const reader = new FileReader();
    reader.onload = (event) => {
      const buffer = event.target.result;
      sendFileState.activeReads--;
      sendFileState.readQueue.set(currentIndex, buffer);
      sendOrderedChunksWebSocket();
    };
    reader.onerror = (err) => {
      console.error('File read error:', err);
      sendFileState.activeReads--;
      cancelActiveTransfer();
    };
    reader.readAsArrayBuffer(slice);
  }
}

function sendOrderedChunksWebSocket() {
  if (isSendingWebSocket) return;
  isSendingWebSocket = true;

  try {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      isSendingWebSocket = false;
      return;
    }
    const WS_BUFFER_THRESHOLD = 1048576;
    
    while (sendFileState.readQueue.has(sendFileState.sendIndex)) {
      if (socket.bufferedAmount >= WS_BUFFER_THRESHOLD) {
        isSendingWebSocket = false;
        setTimeout(sendOrderedChunksWebSocket, 2);
        return;
      }
      
      const index = sendFileState.sendIndex;
      const chunk = sendFileState.readQueue.get(index);
      sendFileState.readQueue.delete(index);
      
      try {
        const targetBytes = sendFileState.targetBytes;
        const payload = new Uint8Array(8 + chunk.byteLength);
        payload.set(targetBytes, 0);
        payload.set(new Uint8Array(chunk), 8);
        
        socket.send(payload.buffer);
        sendFileState.sendIndex++;
        sendFileState.lastBytesSent += chunk.byteLength;
        
        updateProgressUI(sendFileState.lastBytesSent, sendFileState.file.size, sendFileState, true);
        
        if (sendFileState.lastBytesSent >= sendFileState.file.size) {
          showToast('File transfer completed!', 'success');
          addHistoryItem(sendFileState.file.name, sendFileState.file.size, 'sent', 'completed');
          setTimeout(() => {
            resetAllTransferStates();
            closeTransferModal();
          }, 1500);
          isSendingWebSocket = false;
          return;
        }
      } catch (err) {
        console.error('WebSocket send error:', err);
        cancelActiveTransfer();
        isSendingWebSocket = false;
        return;
      }
    }
    sendNextChunksWebSocket();
  } finally {
    isSendingWebSocket = false;
  }
}

function handleRelayedWebSocketChunk(arrayBuffer) {
  processIncomingChunk(receiveFileState.senderPeerId, arrayBuffer);
}

function processIncomingChunk(peerId, data) {
  if (!receiveFileState.senderPeerId || receiveFileState.finalized) return;

  if (!receiveFileState.startTime) {
    receiveFileState.startTime = performance.now();
    receiveFileState.lastTime = receiveFileState.startTime;
    
    document.getElementById('transfer-title').textContent = 'Receiving File';
    const peer = peers.get(receiveFileState.senderPeerId);
    document.getElementById('transfer-peer-info').textContent = `From ${peer ? peer.name : 'Peer'}`;
    
    const badge = document.getElementById('connection-mode-badge');
    if (receiveFileState.useWebSocketRelay) {
      badge.textContent = 'WebSocket Relay (Uses Internet Data)';
      badge.className = 'connection-mode-badge relay';
    } else {
      badge.textContent = 'Direct P2P (0 Internet Data)';
      badge.className = 'connection-mode-badge direct';
    }
    document.getElementById('transfer-modal').classList.add('active');
  }
  
  receiveFileState.receivedSize += data.byteLength;
  
  if (receiveFileState.useStream && navigator.serviceWorker && navigator.serviceWorker.controller) {
    navigator.serviceWorker.controller.postMessage({
      type: 'WRITE_CHUNK',
      streamId: receiveFileState.streamId,
      chunk: data
    }, [data]);
  } else {
    receiveFileState.chunks.push(data);
  }
  
  updateProgressUI(receiveFileState.receivedSize, receiveFileState.fileSize, receiveFileState, false);
  
  if (receiveFileState.receivedSize >= receiveFileState.fileSize) {
    finalizeReceivedFile();
  }
}

function finalizeReceivedFile() {
  if (receiveFileState.finalized) return;
  receiveFileState.finalized = true;

  showToast('File received successfully!', 'success');
  addHistoryItem(receiveFileState.fileName, receiveFileState.fileSize, 'received', 'completed');
  
  if (receiveFileState.useStream && navigator.serviceWorker && navigator.serviceWorker.controller) {
    navigator.serviceWorker.controller.postMessage({
      type: 'CLOSE_STREAM',
      streamId: receiveFileState.streamId
    });
  } else {
    const blob = new Blob(receiveFileState.chunks);
    
    if (isCapacitor) {
      const formData = new FormData();
      formData.append('file', blob);
      const uploadUrl = `http://localhost:8080/api/register-file?name=${encodeURIComponent(receiveFileState.fileName)}&mime=${encodeURIComponent(blob.type || 'application/octet-stream')}`;
      
      fetch(uploadUrl, {
        method: 'POST',
        body: formData
      })
      .then(res => res.json())
      .then(data => {
        if (data.status === 'OK') {
          window.location.href = data.downloadUrl;
        } else {
          throw new Error('Registration failed');
        }
      })
      .catch(err => {
        console.error('Failed to trigger local download fallback:', err);
        const url = URL.createObjectURL(blob);
        window.location.href = url;
      });
    } else {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = receiveFileState.fileName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(() => { URL.revokeObjectURL(url); }, 1000);
    }
  }
  
  setTimeout(() => {
    closeTransferModal();
    receiveFileState.chunks = [];
  }, 1000);
}

function updateProgressUI(current, total, state, isSender) {
  const percent = Math.min(Math.round((current / total) * 100), 100);
  
  const progressBar = document.getElementById('progress-bar');
  if (progressBar) progressBar.style.width = `${percent}%`;
  const progressPercent = document.getElementById('progress-percent');
  if (progressPercent) progressPercent.textContent = `${percent}%`;
  const transBytes = document.getElementById('transferred-bytes');
  if (transBytes) transBytes.textContent = `${formatBytes(current)} / ${formatBytes(total)}`;
  
  const now = performance.now();
  const timeElapsed = (now - state.startTime) / 1000;
  
  if (timeElapsed > 0.5) {
    const bytesTransferred = current;
    const speedBytesPerSec = bytesTransferred / timeElapsed;
    const transSpeed = document.getElementById('transfer-speed');
    if (transSpeed) transSpeed.textContent = `${formatBytes(speedBytesPerSec)}/s`;
    
    const remainingBytes = total - current;
    const timeRem = document.getElementById('time-remaining');
    if (timeRem) {
      if (speedBytesPerSec > 0) {
        const remainingSecs = Math.round(remainingBytes / speedBytesPerSec);
        timeRem.textContent = formatTime(remainingSecs);
      } else {
        timeRem.textContent = 'Calculating...';
      }
    }
  } else {
    const transSpeed = document.getElementById('transfer-speed');
    if (transSpeed) transSpeed.textContent = '0 KB/s';
    const timeRem = document.getElementById('time-remaining');
    if (timeRem) timeRem.textContent = 'Calculating...';
  }
  
  updateLiveConnectionModeBadge(isSender);
}

let lastLiveBadgeUpdate = 0;
async function updateLiveConnectionModeBadge(isSender) {
  const now = Date.now();
  if (now - lastLiveBadgeUpdate < 2000) return;
  lastLiveBadgeUpdate = now;

  const badge = document.getElementById('connection-mode-badge');
  if (!badge) return;

  if (sendFileState.useWebSocketRelay || receiveFileState.useWebSocketRelay) {
    badge.textContent = 'WebSocket Relay (Uses Internet Data)';
    badge.className = 'connection-mode-badge relay';
    return;
  }

  const peerId = isSender ? sendFileState.targetPeerId : receiveFileState.senderPeerId;
  const peerInfo = peers.get(peerId);
  if (!peerInfo || !peerInfo.pc) return;

  try {
    const stats = await peerInfo.pc.getStats();
    let localType = '';
    let remoteType = '';
    let localIp = '';
    let remoteIp = '';
    
    let activePair = null;
    stats.forEach(report => {
      if (report.type === 'transport' && report.selectedCandidatePairId) {
        activePair = stats.get(report.selectedCandidatePairId);
      }
    });
    
    if (!activePair) {
      stats.forEach(report => {
        if (report.type === 'candidate-pair' && (report.selected || report.nominated || report.state === 'succeeded')) {
          activePair = report;
        }
      });
    }
    
    if (activePair) {
      const localCand = stats.get(activePair.localCandidateId);
      const remoteCand = stats.get(activePair.remoteCandidateId);
      if (localCand && remoteCand) {
        localType = localCand.candidateType;
        remoteType = remoteCand.candidateType;
        localIp = localCand.ip || localCand.address || localCand.ipAddress || '';
        remoteIp = remoteCand.ip || remoteCand.address || remoteCand.ipAddress || '';
      }
    }

    const isLocalIp = (ip) => {
      if (!ip) return false;
      const cleanIp = ip.trim().toLowerCase();
      return cleanIp.startsWith('192.168.') || 
             cleanIp.startsWith('10.') || 
             cleanIp.startsWith('172.') || 
             cleanIp.startsWith('127.') || 
             cleanIp.startsWith('169.254.') || 
             cleanIp.endsWith('.local') ||
             cleanIp === 'localhost' ||
             cleanIp === '::1';
    };

    const hasRelay = localType === 'relay' || remoteType === 'relay';
    const isLocal = !hasRelay && (
      localType === 'host' || 
      remoteType === 'host' || 
      isLocalIp(localIp) || 
      isLocalIp(remoteIp) ||
      (localIpAddress && !hasRelay)
    );

    if (hasRelay) {
      badge.textContent = 'TURN Relay (Uses Internet Data)';
      badge.className = 'connection-mode-badge relay';
    } else if (isLocal) {
      badge.textContent = 'Direct Local P2P (0 Internet Data)';
      badge.className = 'connection-mode-badge direct';
    } else {
      badge.textContent = 'Direct P2P (Internet, Uses Data)';
      badge.className = 'connection-mode-badge relay';
    }
  } catch (e) {
    console.warn('Error fetching connection stats:', e);
  }
}

function resetAllTransferStates() {
  transferGeneration++;
  sendFileState.activeChannel = null;
  sendFileState.useWebSocketRelay = false;
  sendFileState.file = null;
  sendFileState.targetPeerId = null;
  sendFileState.isPickingFile = false;
  if (sendFileState.readQueue) {
    sendFileState.readQueue.clear();
  }
  sendFileState.readIndex = 0;
  sendFileState.sendIndex = 0;
  sendFileState.offset = 0;
  sendFileState.activeReads = 0;

  if (receiveFileState.useStream && receiveFileState.streamId &&
      navigator.serviceWorker && navigator.serviceWorker.controller) {
    navigator.serviceWorker.controller.postMessage({
      type: 'CANCEL_STREAM',
      streamId: receiveFileState.streamId
    });
    const iframe = document.getElementById('sw-download-iframe');
    if (iframe) {
      iframe.src = 'about:blank';
    }
  }

  receiveFileState.chunks = [];
  receiveFileState.senderPeerId = null;
  receiveFileState.fileName = '';
  receiveFileState.fileSize = 0;
  receiveFileState.receivedSize = 0;
  receiveFileState.useWebSocketRelay = false;
  receiveFileState.useStream = false;
  receiveFileState.streamId = null;
  receiveFileState.finalized = false;
}

function cancelActiveTransfer() {
  let hadActivity = false;

  if (sendFileState.file) {
    hadActivity = true;
    addHistoryItem(sendFileState.file.name, sendFileState.file.size, 'sent', 'cancelled');
    try {
      if (sendFileState.useWebSocketRelay && sendFileState.targetPeerId) {
        sendSignal(sendFileState.targetPeerId, { type: 'ws-cancel' });
      } else if (sendFileState.activeChannel && sendFileState.activeChannel.readyState === 'open') {
        sendFileState.activeChannel.send(JSON.stringify({ type: 'cancel' }));
      }
    } catch(e){}
  }
  
  if (receiveFileState.senderPeerId) {
    hadActivity = true;
    if (receiveFileState.fileName) {
      addHistoryItem(receiveFileState.fileName, receiveFileState.fileSize, 'received', 'cancelled');
    }
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
  }
  
  resetAllTransferStates();
  closeTransferModal();
  if (hadActivity) {
    showToast('Transfer cancelled', 'info');
  }
}

// Peer UI Card Render
function addPeerCardToGrid(peerId, name) {
  const grid = document.getElementById('peer-grid');
  if (!grid) return;

  const emptyState = document.getElementById('empty-state');
  if (emptyState) {
    emptyState.style.display = 'none';
  }
  
  let card = document.getElementById(`peer-${peerId}`);
  if (!card) {
    card = document.createElement('div');
    card.className = 'peer-card';
    card.id = `peer-${peerId}`;
    grid.appendChild(card);
  }
  
  card.className = 'peer-card';
  card.innerHTML = `
    <div class="peer-avatar">${name.substring(0, 2).toUpperCase()}</div>
    <div class="peer-name">${name}</div>
    <div class="peer-desc">Click card to send file</div>
  `;
  
  card.onclick = () => {
    selectAndSendFile(peerId);
  };
  
  updatePeerConnectionBadge(peerId);
  updatePeerCount();
}

function updatePeerConnectionBadge(peerId) {
  const card = document.getElementById(`peer-${peerId}`);
  if (!card) return;
  
  const peerInfo = peers.get(peerId);
  let badgeEl = card.querySelector('.peer-connection-badge');
  if (!badgeEl) {
    badgeEl = document.createElement('span');
    badgeEl.className = 'peer-connection-badge';
    const descEl = card.querySelector('.peer-desc');
    card.insertBefore(badgeEl, descEl);
  }
  
  const forceRelay = document.getElementById('relay-toggle') && document.getElementById('relay-toggle').checked;
  
  if (forceRelay) {
    badgeEl.textContent = 'WebSocket Relay (Uses Data)';
    badgeEl.className = 'peer-connection-badge relay';
  } else if (peerInfo && peerInfo.dc && peerInfo.dc.readyState === 'open') {
    badgeEl.textContent = 'Direct P2P (Free 0-Data)';
    badgeEl.className = 'peer-connection-badge direct';
  } else {
    if (peerInfo && peerInfo.webrtcFailed) {
      badgeEl.textContent = 'Remote (Relay Ready)';
      badgeEl.className = 'peer-connection-badge relay';
    } else {
      badgeEl.textContent = 'Connecting WebRTC P2P...';
      badgeEl.className = 'peer-connection-badge relay';
    }
  }
}

function removePeer(peerId) {
  const card = document.getElementById(`peer-${peerId}`);
  if (card) card.remove();
  
  const peerInfo = peers.get(peerId);
  if (peerInfo) {
    if (peerInfo.dc) peerInfo.dc.close();
    if (peerInfo.pc) peerInfo.pc.close();
    peers.delete(peerId);
  }
  
  updatePeerCount();
  
  if (peers.size === 0) {
    const emptyState = document.getElementById('empty-state');
    if (emptyState) emptyState.style.display = 'flex';
    
    // Redirect room owner/participants back to setup
    window.location.href = `room.html#${roomId}`;
  }
}

function closePeerConnection(peerId) {
  const peerInfo = peers.get(peerId);
  if (peerInfo) {
    if (peerInfo.dc) {
      try { peerInfo.dc.close(); } catch(e){}
      peerInfo.dc = null;
    }
    if (peerInfo.pc) {
      try { peerInfo.pc.close(); } catch(e){}
      peerInfo.pc = null;
    }
    peerInfo.webrtcFailed = true;
    peerInfo.remoteDescSet = false;
    peerInfo.candidateQueue = [];
  }
  const card = document.getElementById(`peer-${peerId}`);
  if (card) {
    card.classList.remove('active');
  }
  updatePeerConnectionBadge(peerId);
}

function handleWebRTCFailure(peerId) {
  closePeerConnection(peerId);
}

function updatePeerCount() {
  const cnt = document.getElementById('peer-count');
  if (cnt) cnt.textContent = `${peers.size} connected`;
}

function updateConnectionStatus(status, text) {
  const indicator = document.getElementById('connection-status');
  if (indicator) {
    indicator.className = `connection-indicator ${status}`;
    const txt = indicator.querySelector('.status-text');
    if (txt) txt.textContent = text;
  }
}

// QR Code Tab Slider update
function updateQRTabSlider() {
  const activeTab = document.querySelector('.qr-tab.active');
  const slider = document.querySelector('.qr-tab-slider');
  if (activeTab && slider) {
    slider.style.left = `${activeTab.offsetLeft}px`;
    slider.style.width = `${activeTab.offsetWidth}px`;
  }
}

// Local IP optimizer
async function enableLocalIPs() {
  const btn = document.getElementById('optimize-p2p-btn');
  if (!btn) return;
  
  if (isCapacitor || isNanoHTTPD) {
    btn.textContent = 'Active';
    btn.className = 'btn btn-primary btn-sm';
    btn.disabled = true;
    showToast('Local P2P already optimized via native server!', 'success');
    return;
  }

  btn.textContent = 'Optimizing...';
  btn.disabled = true;
  
  try {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      throw new Error('MediaDevices API not supported on this context (requires HTTPS or localhost)');
    }
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    stream.getTracks().forEach(track => track.stop());
    
    btn.textContent = 'Active';
    btn.className = 'btn btn-primary btn-sm';
    showToast('Local P2P optimized! Disabling mDNS obfuscation.', 'success');
    
    const oldPeers = Array.from(peers.keys());
    oldPeers.forEach(peerId => {
      removePeer(peerId);
    });
    
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'ping' }));
    }
  } catch (err) {
    console.warn('Microphone permission denied, local IP gathering disabled:', err);
    btn.textContent = 'Failed';
    btn.className = 'btn btn-danger btn-sm';
    btn.disabled = false;
    showToast(`Failed: ${err.message}. Please allow microphone permissions.`, 'danger');
  }
}

function copyRoomLink() {
  const targetUrl = `${window.location.origin}/room.html#${roomId}`;
  navigator.clipboard.writeText(targetUrl).then(() => {
    showToast('Room link copied to clipboard!');
  }).catch(err => {
    console.error('Failed to copy text: ', err);
  });
}

function closeTransferModal() {
  const modal = document.getElementById('transfer-modal');
  if (modal) modal.classList.remove('active');
  const fileInput = document.getElementById('file-input');
  if (fileInput) fileInput.value = '';
}

// QR Code Canvas Generation
let qr = null;
function generateQRCode() {
  const canvas = document.getElementById('qr-code-canvas');
  if (!canvas) return;

  let targetUrl = `${window.location.origin}/room.html#${roomId}`;
  if (qrMode === 'local' && localIpAddress) {
    targetUrl = `http://${localIpAddress}:8080/room.html#${roomId}`;
  }
  
  qr = new QRious({
    element: canvas,
    value: targetUrl,
    size: 260,
    background: '#ffffff',
    foreground: '#0a0c16',
    level: 'H'
  });
  const linkText = document.getElementById('qr-link-text');
  if (linkText) {
    linkText.textContent = targetUrl;
  }
}

// Service worker registration
let serviceWorkerRegistration = null;
if ('serviceWorker' in navigator && isPeersPage) {
  navigator.serviceWorker.register('/sw.js')
    .then(reg => {
      serviceWorkerRegistration = reg;
      console.log('Service Worker registered successfully');
    })
    .catch(err => {
      console.warn('Service Worker registration failed:', err);
    });
}

// DOM Setup & Action Binding
document.addEventListener('DOMContentLoaded', () => {
  setupRoomId();
  
  if (isIndexPage) {
    // Landing Page Specific logic
    const startSharingBtn = document.getElementById('start-sharing-btn');
    if (startSharingBtn) {
      startSharingBtn.addEventListener('click', () => {
        window.location.href = `room.html#${roomId}`;
      });
    }

    // Dismiss APK Prompt
    const closeApkPromptBtn = document.getElementById('close-apk-prompt-btn');
    if (closeApkPromptBtn) {
      closeApkPromptBtn.addEventListener('click', () => {
        document.getElementById('apk-download-modal').classList.remove('active');
        localStorage.setItem('apk_prompt_dismissed', 'true');
        window.location.href = `room.html#${roomId}`;
      });
    }

    if (!isCapacitor && !localStorage.getItem('apk_prompt_dismissed')) {
      setTimeout(() => {
        const apkModal = document.getElementById('apk-download-modal');
        if (apkModal) apkModal.classList.add('active');
      }, 3000);
    }
  }

  if (isRoomPage || isPeersPage) {
    // Fetch LAN IP for app contexts
    if (isCapacitor) {
      document.body.classList.add('is-app');
      const apkBanner = document.querySelector('.apk-download-banner');
      if (apkBanner) apkBanner.style.display = 'none';

      fetch('http://localhost:8080/api/ip')
        .then(res => res.json())
        .then(data => {
          if (data && data.ip) {
            localIpAddress = data.ip;
            generateQRCode();
          }
        })
        .catch(err => console.error('Failed to fetch local IP:', err));
    } else if (isNanoHTTPD) {
      fetch('/api/peer-ip')
        .then(res => res.json())
        .then(data => {
          if (data && data.ip) {
            localIpAddress = data.ip;
            generateQRCode();
          }
        })
        .catch(err => console.warn('Failed to fetch peer IP:', err));
    }

    connectSignaling();
  }

  if (isRoomPage) {
    generateQRCode();
    
    // QR Tabs handler
    const tabLocal = document.getElementById('qr-tab-local');
    const tabInternet = document.getElementById('qr-tab-internet');
    if (tabLocal && tabInternet) {
      tabLocal.addEventListener('click', () => {
        if (qrMode === 'local') return;
        qrMode = 'local';
        tabLocal.classList.add('active');
        tabInternet.classList.remove('active');
        updateQRTabSlider();
        generateQRCode();
      });
      tabInternet.addEventListener('click', () => {
        if (qrMode === 'internet') return;
        qrMode = 'internet';
        tabInternet.classList.add('active');
        tabLocal.classList.remove('active');
        updateQRTabSlider();
        generateQRCode();
      });
      window.addEventListener('resize', updateQRTabSlider);
    }
    
    setTimeout(updateQRTabSlider, 100);

    // Join room
    const joinBtn = document.getElementById('join-room-btn');
    const joinInput = document.getElementById('join-room-input');
    if (joinBtn && joinInput) {
      const performJoin = () => {
        const val = joinInput.value.trim().toLowerCase();
        if (val) {
          window.location.href = `room.html#${val}`;
          window.location.reload();
        }
      };
      joinBtn.addEventListener('click', performJoin);
      joinInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performJoin();
      });
    }

    // QR Code scanner triggers
    const startScanBtn = document.getElementById('start-scan-btn');
    if (startScanBtn) {
      startScanBtn.addEventListener('click', () => {
        document.getElementById('scanner-modal').classList.add('active');
        // Setup Html5Qrcode
        if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
          navigator.mediaDevices.getUserMedia({ video: true })
            .then(stream => {
              stream.getTracks().forEach(t => t.stop());
              launchScanner();
            })
            .catch(err => {
              showToast("Camera permission denied.", "danger");
              document.getElementById('scanner-modal').classList.remove('active');
            });
        } else {
          launchScanner();
        }
      });
    }

    const closeScannerBtn = document.getElementById('close-scanner-btn');
    if (closeScannerBtn) {
      closeScannerBtn.addEventListener('click', () => {
        if (html5QrcodeScanner) {
          html5QrcodeScanner.stop().then(() => {
            html5QrcodeScanner = null;
            document.getElementById('scanner-modal').classList.remove('active');
          }).catch(e => {
            html5QrcodeScanner = null;
            document.getElementById('scanner-modal').classList.remove('active');
          });
        } else {
          document.getElementById('scanner-modal').classList.remove('active');
        }
      });
    }

    // IP optimizer
    const optimizeBtn = document.getElementById('optimize-p2p-btn');
    if (optimizeBtn) {
      optimizeBtn.addEventListener('click', enableLocalIPs);
      if (isCapacitor || isNanoHTTPD) {
        optimizeBtn.textContent = 'Active';
        optimizeBtn.className = 'btn btn-primary btn-sm';
        optimizeBtn.disabled = true;
      }
    }

    document.getElementById('copy-room-btn').addEventListener('click', copyRoomLink);
  }

  if (isPeersPage) {
    loadHistory();
    document.getElementById('copy-room-btn').addEventListener('click', copyRoomLink);
    document.getElementById('invite-btn').addEventListener('click', copyRoomLink);
    document.getElementById('clear-history-btn').addEventListener('click', clearHistory);
    document.getElementById('cancel-transfer-btn').addEventListener('click', cancelActiveTransfer);

    // Relay toggle listener
    const relayToggle = document.getElementById('relay-toggle');
    if (relayToggle) {
      relayToggle.addEventListener('change', () => {
        peers.forEach((peer, id) => updatePeerConnectionBadge(id));
      });
    }

    // Direct app-to-app vs WebRTC file transfer trigger
    document.getElementById('file-input').addEventListener('change', async (e) => {
      sendFileState.isPickingFile = false;
      const file = e.target.files[0];
      if (!file || !sendFileState.targetPeerId) return;

      sendFileState.file = file;
      const forceRelay = document.getElementById('relay-toggle') && document.getElementById('relay-toggle').checked;
      const peer = peers.get(sendFileState.targetPeerId);

      const isAppToApp = isCapacitor && peer && peer.isCapacitor;
      if (isAppToApp && !forceRelay) {
        // Enforce subnet check
        const myOctets = localIpAddress.split('.').slice(0, 3).join('.');
        const peerOctets = peer.localIp.split('.').slice(0, 3).join('.');
        if (myOctets !== peerOctets) {
          const modal = document.getElementById('network-alert-modal');
          if (modal) modal.classList.add('active');
          return;
        }

        document.getElementById('transfer-title').textContent = 'Preparing Transfer';
        document.getElementById('transfer-peer-info').textContent = `Uploading local file to server...`;
        document.getElementById('transfer-speed').textContent = '-';
        document.getElementById('time-remaining').textContent = '-';
        document.getElementById('progress-bar').style.width = '0%';
        document.getElementById('progress-percent').textContent = '0%';
        document.getElementById('transferred-bytes').textContent = `File: ${file.name} (${formatBytes(file.size)})`;
        document.getElementById('transfer-modal').classList.add('active');

        const formData = new FormData();
        formData.append('file', file);
        const uploadUrl = `http://localhost:8080/api/register-file?name=${encodeURIComponent(file.name)}&mime=${encodeURIComponent(file.type)}`;
        
        fetch(uploadUrl, {
          method: 'POST',
          body: formData
        })
        .then(r => r.json())
        .then(data => {
          if (data.status === 'OK') {
            sendSignal(sendFileState.targetPeerId, {
              type: 'direct-download',
              name: file.name,
              size: file.size,
              downloadUrl: data.downloadUrl
            });
            document.getElementById('transfer-title').textContent = 'File Shared';
            document.getElementById('transfer-peer-info').textContent = `Shared with ${peer.name} (Direct App Transfer)`;
            const badge = document.getElementById('connection-mode-badge');
            badge.textContent = 'Direct App-to-App Link';
            badge.className = 'connection-mode-badge direct';
            document.getElementById('progress-bar').style.width = '100%';
            document.getElementById('progress-percent').textContent = '100%';
            document.getElementById('transferred-bytes').textContent = `Link sent to peer!`;
            showToast('App-to-App direct download link sent!', 'success');
            addHistoryItem(file.name, file.size, 'sent', 'completed');
            setTimeout(closeTransferModal, 2000);
          } else {
            throw new Error('Registration failed');
          }
        })
        .catch(err => {
          console.warn('Local Java server upload failed, falling back to WebRTC:', err);
          startWebRTCTransfer(file, peer, forceRelay);
        });
      } else {
        startWebRTCTransfer(file, peer, forceRelay);
      }
    });

    // Accept / Decline Buttons
    document.getElementById('accept-file-btn').addEventListener('click', async () => {
      document.getElementById('incoming-modal').classList.remove('active');
      
      if (receiveFileState.isDirectDownload) {
        showToast('Starting direct download...', 'success');
        if (isCapacitor) {
          window.location.href = receiveFileState.downloadUrl;
        } else {
          let iframe = document.getElementById('sw-download-iframe');
          if (!iframe) {
            iframe = document.createElement('iframe');
            iframe.id = 'sw-download-iframe';
            iframe.style.display = 'none';
            document.body.appendChild(iframe);
          }
          iframe.src = receiveFileState.downloadUrl;
        }
        addHistoryItem(receiveFileState.fileName, receiveFileState.fileSize, 'received', 'completed');
        sendSignal(receiveFileState.senderPeerId, { type: 'ws-accept' });
        return;
      }
      
      const sw = navigator.serviceWorker && navigator.serviceWorker.controller;
      if (sw) {
        const messageChannel = new MessageChannel();
        sw.postMessage({
          type: 'CREATE_STREAM',
          streamId: receiveFileState.streamId,
          name: receiveFileState.fileName,
          size: receiveFileState.fileSize
        }, [messageChannel.port2]);

        await new Promise((resolve) => {
          messageChannel.port1.onmessage = (e) => resolve(e.data);
        });

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
      }
      
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
      if (receiveFileState.isDirectDownload) {
        sendSignal(receiveFileState.senderPeerId, { type: 'ws-decline' });
        return;
      }
      if (receiveFileState.useWebSocketRelay) {
        sendSignal(receiveFileState.senderPeerId, { type: 'ws-decline' });
      } else {
        const peer = peers.get(receiveFileState.senderPeerId);
        if (peer && peer.dc && peer.dc.readyState === 'open') {
          peer.dc.send(JSON.stringify({ type: 'decline' }));
        }
      }
    });
  }

  // Network Alert close
  const closeNetAlertBtn = document.getElementById('close-network-alert-btn');
  if (closeNetAlertBtn) {
    closeNetAlertBtn.addEventListener('click', () => {
      document.getElementById('network-alert-modal').classList.remove('active');
    });
  }
});

// WebRTC Transfer initialization helper
async function startWebRTCTransfer(file, peer, forceRelay) {
  if ((!peer || !peer.dc || peer.dc.readyState !== 'open') && !forceRelay) {
    closePeerConnection(sendFileState.targetPeerId);
    initiatePeerConnection(sendFileState.targetPeerId);
  }
  
  document.getElementById('transfer-title').textContent = 'Waiting for Accept';
  document.getElementById('transfer-peer-info').textContent = `Waiting for ${peer ? peer.name : 'Peer'} to accept...`;
  document.getElementById('transfer-speed').textContent = '-';
  document.getElementById('time-remaining').textContent = '-';
  document.getElementById('progress-bar').style.width = '0%';
  document.getElementById('progress-percent').textContent = '0%';
  document.getElementById('transferred-bytes').textContent = `File: ${file.name} (${formatBytes(file.size)})`;
  document.getElementById('transfer-modal').classList.add('active');

  if (!forceRelay) {
    const dcReady = await new Promise(resolve => {
      const deadline = Date.now() + 8000;
      const poll = () => {
        const p = peers.get(sendFileState.targetPeerId);
        if (p && p.dc && p.dc.readyState === 'open') return resolve(true);
        if (Date.now() >= deadline) return resolve(false);
        setTimeout(poll, 100);
      };
      poll();
    });
  }

  const freshPeer = peers.get(sendFileState.targetPeerId);
  const badge = document.getElementById('connection-mode-badge');

  if (freshPeer && freshPeer.dc && freshPeer.dc.readyState === 'open' && !forceRelay) {
    sendFileState.useWebSocketRelay = false;
    badge.textContent = 'Will use Direct P2P (0 Internet Data)';
    badge.className = 'connection-mode-badge direct';
    freshPeer.dc.send(JSON.stringify({
      type: 'meta',
      name: file.name,
      size: file.size
    }));
  } else {
    // If both are app users, block WebSocket fallback
    if (isCapacitor && freshPeer && freshPeer.isCapacitor) {
      showToast('WebSocket relay blocked for App-to-App. Same local network required.', 'danger');
      closeTransferModal();
      return;
    }

    sendFileState.useWebSocketRelay = true;
    badge.textContent = 'Will use WebSocket Relay (Uses Internet Data)';
    badge.className = 'connection-mode-badge relay';
    showToast('P2P not ready — relaying via WebSocket...', 'info');
    sendSignal(sendFileState.targetPeerId, {
      type: 'ws-meta',
      name: file.name,
      size: file.size
    });
  }
}

function launchScanner() {
  try {
    html5QrcodeScanner = new Html5Qrcode("qr-reader");
    const config = { fps: 10, qrbox: { width: 220, height: 220 } };
    
    html5QrcodeScanner.start(
      { facingMode: "environment" },
      config,
      (decodedText) => {
        try {
          let scannedRoomId = '';
          if (decodedText.includes('#')) {
            scannedRoomId = decodedText.split('#').pop().trim();
          } else {
            scannedRoomId = decodedText.trim();
          }
          
          if (scannedRoomId) {
            showToast(`Joining room: ${scannedRoomId}`);
            if (html5QrcodeScanner) {
              html5QrcodeScanner.stop().then(() => {
                html5QrcodeScanner = null;
                document.getElementById('scanner-modal').classList.remove('active');
                window.location.href = `room.html#${scannedRoomId}`;
                window.location.reload();
              });
            }
          }
        } catch (err) {
          console.error("Failed to parse scanned URL:", err);
        }
      },
      (errorMessage) => {}
    ).catch(err => {
      showToast("Camera access failed.", "danger");
      document.getElementById('scanner-modal').classList.remove('active');
    });
  } catch (err) {
    console.error("Scanner init failed:", err);
    document.getElementById('scanner-modal').classList.remove('active');
  }
}

// Reconnect on visibility change
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    if (!socket || socket.readyState === WebSocket.CLOSED || socket.readyState === WebSocket.CLOSING) {
      connectSignaling();
    }
  }
});

window.addEventListener('focus', () => {
  if (!socket || socket.readyState === WebSocket.CLOSED || socket.readyState === WebSocket.CLOSING) {
    connectSignaling();
  }
});

// Formatting and Toast helpers
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

function showToast(message, type = 'success') {
  const container = document.getElementById('toast-container');
  if (!container) return;
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  
  let icon = '';
  if (type === 'success') {
    icon = '<svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none"><polyline points="20 6 9 17 4 12"></polyline></svg>';
  } else {
    icon = '<svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>';
  }
  
  toast.innerHTML = `${icon} <span>${message}</span>`;
  container.appendChild(toast);
  
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

// History List management
let transferHistory = [];
function loadHistory() {
  try {
    const data = localStorage.getItem('ajshare_history');
    if (data) {
      transferHistory = JSON.parse(data);
    }
  } catch(e) {}
  renderHistory();
}

function saveHistory() {
  try {
    localStorage.setItem('ajshare_history', JSON.stringify(transferHistory));
  } catch(e) {}
}

function addHistoryItem(name, size, type, status) {
  const item = {
    id: Math.random().toString(36).substring(2, 9),
    name: name,
    size: size,
    type: type,
    status: status,
    timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  };
  transferHistory.unshift(item);
  if (transferHistory.length > 30) {
    transferHistory.pop();
  }
  saveHistory();
  renderHistory();
}

function renderHistory() {
  const list = document.getElementById('history-list');
  const emptyState = document.getElementById('empty-history');
  if (!list) return;
  
  const items = list.querySelectorAll('.history-item');
  items.forEach(el => el.remove());
  
  if (transferHistory.length === 0) {
    if (emptyState) emptyState.style.display = 'block';
    return;
  }
  
  if (emptyState) emptyState.style.display = 'none';
  
  transferHistory.forEach(item => {
    const row = document.createElement('div');
    row.className = 'history-item';
    const iconChar = item.type === 'sent' ? '↑' : '↓';
    
    row.innerHTML = `
      <div class="history-left">
        <div class="history-icon-badge ${item.type}">${iconChar}</div>
        <div class="history-details">
          <h4 class="history-file-name" style="cursor: pointer; text-decoration: underline;">${escapeHTML(item.name)}</h4>
          <p>${formatBytes(item.size)} • ${item.timestamp}</p>
        </div>
      </div>
      <div class="history-right">
        <span class="history-status ${item.status}">${item.status}</span>
      </div>
    `;
    list.appendChild(row);
  });
}

function clearHistory() {
  transferHistory = [];
  saveHistory();
  renderHistory();
  showToast('Transfer history cleared');
}

function escapeHTML(str) {
  return str.replace(/[&<>'"]/g, tag => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    "'": '&#39;',
    '"': '&quot;'
  }[tag] || tag));
}
