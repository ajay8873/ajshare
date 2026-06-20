// script.js
// AJShare Client Logic

// Configuration
const CHUNK_SIZE = 131072; // 128KB chunks for high performance WebRTC direct P2P transfer
const BUFFER_THRESHOLD = 2097152; // 2MB buffer to optimize throughput speed
const PING_INTERVAL = 10000; // 10 seconds — keeps signaling alive even during file picker pauses

// Application State
let roomId = '';
let socket = null;
let myId = '';
let peers = new Map(); // peerId -> { pc, dc, name, deviceType }
let pingIntervalId = null;
let qrMode = 'local'; // 'local' or 'internet'
let reconnectTimeoutId = null;
let localIpAddress = '';
let pendingSignals = [];

// Transfer generation counter — increments on every reset so stale async
// callbacks from a previous transfer can detect they are outdated and bail out.
let transferGeneration = 0;
let isSendingChunks = false;
let isSendingWebSocket = false;

// File Transfer State (Sender)
let sendFileState = {
  file: null,
  offset: 0,
  targetPeerId: null,
  startTime: null,
  lastBytesSent: 0,
  lastTime: null,
  activeChannel: null,
  activeReads: 0, // Track concurrent disk reads in flight
  readQueue: new Map(), // index -> ArrayBuffer
  readIndex: 0,         // Index of the next chunk we slice/read from disk
  sendIndex: 0,         // Index of the next chunk we need to send
  useWebSocketRelay: false,
  isPickingFile: false  // true while the native file picker is open
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
  lastTime: null,
  finalized: false  // Guard against double-finalization
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
document.addEventListener('DOMContentLoaded', async () => {
  setupRoom();
  loadHistory();
  
  const isCapacitor = !!window.Capacitor || (window.location.hostname === 'localhost' && !window.location.port);
  // Detect if this is the PC browser connected to the phone's local NanoHTTPD server
  const isNanoHTTPD = !isCapacitor && window.location.port === '8080' && window.location.hostname !== 'localhost';

  if (isCapacitor) {
    // Phone side: fetch the phone's own LAN IP
    try {
      const res = await fetch('http://localhost:8080/api/ip');
      const data = await res.json();
      if (data && data.ip) {
        localIpAddress = data.ip;
        console.log('Phone local IP:', localIpAddress);
      }
    } catch (err) {
      console.error('Failed to fetch local IP during startup:', err);
    }
  } else if (isNanoHTTPD) {
    // PC side: ask the phone server what IP the PC is connecting from.
    // This lets us rewrite mDNS-masked ICE candidates (d7a34b12.local → 192.168.1.x)
    // so the phone can reach the PC directly instead of going through TURN.
    try {
      const res = await fetch('/api/peer-ip');
      const data = await res.json();
      if (data && data.ip) {
        localIpAddress = data.ip;
        console.log('PC LAN IP (from phone server):', localIpAddress);
      }
    } catch (err) {
      console.warn('Failed to fetch peer IP — ICE candidate rewriting disabled:', err);
    }
  }
  
  generateQRCode();
  connectSignaling();
  setupUIEventListeners();
  
  if (isCapacitor || isNanoHTTPD) {
    const btn = document.getElementById('optimize-p2p-btn');
    if (btn) {
      btn.textContent = 'Active';
      btn.className = 'btn btn-primary btn-sm';
      btn.disabled = true;
    }
  }
  
  setTimeout(updateQRTabSlider, 100);
});

// Reconnect instantly when returning to the app
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    if (!socket || socket.readyState === WebSocket.CLOSED || socket.readyState === WebSocket.CLOSING) {
      console.log('App returned to foreground, reconnecting signaling...');
      connectSignaling();
    }
  }
});

window.addEventListener('focus', () => {
  if (!socket || socket.readyState === WebSocket.CLOSED || socket.readyState === WebSocket.CLOSING) {
    console.log('App focused, reconnecting signaling...');
    connectSignaling();
  }
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
  if (reconnectTimeoutId) {
    clearTimeout(reconnectTimeoutId);
    reconnectTimeoutId = null;
  }
  const host = window.location.host;
  
  // Detect if running in a local development environment
  const isDev = (!host || 
                host.startsWith('localhost') || 
                host.startsWith('127.0.0.1')) && 
                !window.Capacitor && 
                !(window.location.hostname === 'localhost' && !window.location.port);

  // Use secure WebSocket only when page is served over HTTPS.
  // When served over HTTP (e.g. from the phone's local NanoHTTPD server),
  // we MUST use ws:// — Chrome blocks wss:// connections from http:// pages (Mixed Content).
  let wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  let wsHost = 'ajshare.mehtaajay8873.workers.dev';

  if (isDev && window.location.protocol !== 'file:') {
    wsHost = host;
  } else if (window.location.port === '8080' && window.location.hostname !== 'localhost') {
    // PC browser connected to the phone's local NanoHTTPD server.
    // Route signaling through the phone's WebSocket proxy on port 8081.
    // ws://192.168.1.14:8081 → phone proxies → wss://ajshare.mehtaajay8873.workers.dev
    wsHost = window.location.hostname + ':8081';
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

    // Send any pending signals
    while (pendingSignals.length > 0) {
      const pending = pendingSignals.shift();
      sendSignal(pending.target, pending.signal);
    }

    // If we reconnected mid-transfer (e.g. after file picker paused the WebView),
    // and the target peer is still in the room, re-initiate WebRTC toward them.
    // The welcome handler will do this automatically, but log it for clarity.
    if (sendFileState.isPickingFile && sendFileState.targetPeerId) {
      console.log('Signaling reconnected during file pick. Will re-establish P2P after reconnect.');
    }
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
    if (!sendFileState.isPickingFile) {
      showToast('Connection lost. Reconnecting...', 'info');
    }
    clearInterval(pingIntervalId);
    
    // Do NOT remove active WebRTC peers on signaling disconnect
    // WebRTC connections can function/survive independently of the signaling channel!
    
    if (!reconnectTimeoutId) {
      // Reconnect instantly if we dropped during file picking; otherwise wait 5s
      const delay = sendFileState.isPickingFile ? 0 : 5000;
      reconnectTimeoutId = setTimeout(connectSignaling, delay);
    }
  });
}

function handleSignalingMessage(msg) {
  switch (msg.type) {
    case 'welcome':
      myId = msg.clientId;
      document.getElementById('my-peer-id').textContent = `${getDeviceType()} • ID: ${myId}`;
      document.getElementById('my-avatar').textContent = myId.substring(0, 2).toUpperCase();
      
      // Prune any existing peers that are no longer in the room
      const activePeers = new Set(msg.peers || []);
      peers.forEach((peer, peerId) => {
        // Do NOT prune the active transfer target/sender peer, or any peer with an open data channel
        if (peerId === sendFileState.targetPeerId || peerId === receiveFileState.senderPeerId) return;
        if (peer.dc && peer.dc.readyState === 'open') return;
        
        if (!activePeers.has(peerId)) {
          removePeer(peerId);
        }
      });
      
      // Pre-connect to all existing peers in the room
      if (msg.peers && msg.peers.length > 0) {
        msg.peers.forEach(peerId => {
          initiatePeerConnection(peerId);
        });
      }
      break;
      
    case 'peer-joined':
      // Only show toast if we don't already have this peer (avoid repeated toasts on reconnect)
      // AND we are not currently in a file-picking flow (phone reconnects mid-pick).
      if (!peers.has(msg.peerId) && !sendFileState.isPickingFile) {
        showToast('New peer entered the room', 'info');
      }
      // Connection will be initiated by the newly joined peer via the 'welcome' packet.
      // The existing peer just waits to receive the incoming offer signal.
      break;
      
    case 'peer-left':
      const peer = peers.get(msg.peerId);
      if (peer && (peer.isSelectingFile || (peer.dc && peer.dc.readyState === 'open'))) {
        console.log(`Peer ${msg.peerId} left signaling but is selecting a file or WebRTC is active — keeping peer.`);
      } else {
        showToast('A peer left the room', 'info');
        removePeer(msg.peerId);
      }
      break;
      
    case 'signal':
      const sig = msg.signal;
      console.log(`Received signal type "${sig.type}" from peer ${msg.sender}`);
      if (sig.type === 'ws-meta') {
        console.log('Handling incoming ws-meta signal, showing accept modal...');
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
        console.log('Peer accepted WebSocket relay. Starting transmission...');
        // Callee accepted our WebSocket Relay request!
        startFileTransmissionWebSocket();
      } else if (sig.type === 'ws-decline') {
        console.log('Peer declined WebSocket relay.');
        showToast('Peer declined the file transfer', 'danger');
        closeTransferModal();
      } else if (sig.type === 'ws-cancel') {
        console.log('WebSocket relay transfer was cancelled by peer.');
        showToast('Transfer was cancelled by peer', 'danger');
        resetAllTransferStates();
        closeTransferModal();
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
  const existingPeer = peers.get(peerId);
  if (existingPeer) {
    if (existingPeer.dc && (existingPeer.dc.readyState === 'open' || existingPeer.dc.readyState === 'connecting')) return;
    closePeerConnection(peerId);
  }
  
  const pc = new RTCPeerConnection(rtcConfig);
  const friendlyName = existingPeer ? existingPeer.name : generateFriendlyName();
  const deviceType = existingPeer ? existingPeer.deviceType : 'Device';
  
  const peerInfo = {
    pc: pc,
    dc: null,
    name: friendlyName,
    deviceType: deviceType,
    candidateQueue: [], // Queue candidates until remote desc is set
    remoteDescSet: false,
    webrtcFailed: false
  };
  peers.set(peerId, peerInfo);
  
  pc.oniceconnectionstatechange = () => {
    console.log(`ICE Connection State with ${peerId}: ${pc.iceConnectionState}`);
    if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed') {
      handleWebRTCFailure(peerId);
    }
  };
  pc.onconnectionstatechange = () => {
    console.log(`Connection State with ${peerId}: ${pc.connectionState}`);
    if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      handleWebRTCFailure(peerId);
    }
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
  if (peerInfo) {
    peerInfo.webrtcFailed = false;
  }
  
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
      remoteDescSet: false,
      webrtcFailed: false
    };
    peers.set(peerId, peerInfo);
    
    pc.oniceconnectionstatechange = () => {
      console.log(`ICE Connection State with ${peerId}: ${pc.iceConnectionState}`);
      if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'closed') {
        handleWebRTCFailure(peerId);
      }
    };
    pc.onconnectionstatechange = () => {
      console.log(`Connection State with ${peerId}: ${pc.connectionState}`);
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
        handleWebRTCFailure(peerId);
      }
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
    
    // Extract remote IP if it's a host candidate to dynamically discover our own local IP
    const parts = signal.candidate.candidate.split(' ');
    if (parts.length >= 8 && parts[7] === 'host') {
      const remoteIp = parts[4];
      if (remoteIp && !remoteIp.endsWith('.local') && !localIpAddress && window.location.protocol !== 'https:') {
        console.log(`Detected remote host raw IP: ${remoteIp}. Querying for local IP...`);
        fetch(`http://${remoteIp}:8080/api/peer-ip`)
          .then(res => res.json())
          .then(data => {
            if (data && data.ip) {
              localIpAddress = data.ip;
              console.log(`PC dynamically resolved its active local IP as: ${localIpAddress}`);
            }
          })
          .catch(err => {
            console.warn('Failed to query remote local server for peer-ip:', err);
          });
      }
    }

    if (peerInfo.remoteDescSet) {
      pc.addIceCandidate(new RTCIceCandidate(signal.candidate))
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
      peerInfo.pc.addIceCandidate(new RTCIceCandidate(candidate))
        .catch(err => console.error('Error adding queued ICE candidate:', err));
    });
    peerInfo.candidateQueue = [];
  }
}

function setupDataChannel(peerId, dc) {
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = 524288; // 512KB low threshold to keep buffer filled incrementally
  
  dc.onopen = () => {
    const card = document.getElementById(`peer-${peerId}`);
    if (card) {
      card.classList.add('active');
    }
    updatePeerConnectionBadge(peerId);
    
    // Dynamically upgrade pending WebSocket relay requests to Direct P2P
    if (sendFileState.targetPeerId === peerId && sendFileState.file && sendFileState.useWebSocketRelay) {
      console.log(`WebRTC channel opened with target peer ${peerId}. Upgrading pending transfer to Direct P2P.`);
      sendFileState.useWebSocketRelay = false;
      sendFileState.activeChannel = dc;
      
      const badge = document.getElementById('connection-mode-badge');
      if (badge) {
        badge.textContent = 'Will use Direct P2P (0 Internet Data)';
        badge.className = 'connection-mode-badge direct';
      }
      
      try {
        dc.send(JSON.stringify({
          type: 'meta',
          name: sendFileState.file.name,
          size: sendFileState.file.size
        }));
      } catch (e) {
        console.error('Failed to send upgraded meta over WebRTC:', e);
      }
    }
  };
  
  dc.onclose = () => {
    console.log(`WebRTC Data Channel with ${peerId} closed.`);
    handleWebRTCFailure(peerId);
  };
  
  dc.onmessage = (event) => {
    handleDataChannelMessage(peerId, event.data);
  };
}

function rewriteSdpOrCandidate(signal) {
  if (!localIpAddress) return signal;
  
  // Create a deep copy to avoid mutating the original WebRTC objects
  const signalCopy = JSON.parse(JSON.stringify(signal));
  
  const injectLocalHostCandidate = (candStr) => {
    if (!localIpAddress) return null;
    const parts = candStr.split(' ');
    if (parts.length < 8) return null;

    const typ = parts[7];
    const ip  = parts[4];

    // ── Case 1: Host candidate ──────
    // Replace the gathered host IP (which might be an mDNS alias or an inactive Wi-Fi IP)
    // with the actual active LAN/Hotspot IP, keeping the port intact.
    if (typ === 'host' && ip) {
      parts[4] = localIpAddress;
      const rewritten = parts.join(' ');
      console.log('Rewrote host candidate to LAN IP:', rewritten);
      return rewritten;
    }

    // ── Case 2: STUN srflx (only synthesize when rport is non-zero) ──────────
    // Chrome privacy mode sets raddr 0.0.0.0 rport 0 → skip synthesis in that
    // case (the mDNS candidate above already provides the correct local entry).
    if (typ === 'srflx') {
      let rport = null;
      for (let i = 8; i < parts.length - 1; i++) {
        if (parts[i] === 'rport' && parts[i + 1] !== '0') {
          rport = parts[i + 1];
          break;
        }
      }
      if (!rport) return null; // rport 0 → cannot synthesize a valid candidate
      const foundation = parts[0].includes(':') ? parts[0].split(':')[1] : parts[0];
      const newCand = `candidate:${foundation} ${parts[1]} udp 2122260223 ${localIpAddress} ${rport} typ host`;
      console.log('Synthesized local host candidate from STUN rport:', newCand);
      return newCand;
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
            console.log('Injected synthesized host candidate into SDP:', newCandLine);
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
          
        case 'selecting-file':
          console.log(`Peer ${peerId} is selecting a file.`);
          peerInfo.isSelectingFile = true;
          if (peerInfo.selectingFileTimeoutId) clearTimeout(peerInfo.selectingFileTimeoutId);
          peerInfo.selectingFileTimeoutId = setTimeout(() => {
            peerInfo.isSelectingFile = false;
          }, 30000);
          break;
          
        case 'cancel':
          showToast('Transfer was cancelled by peer', 'danger');
          resetAllTransferStates();
          closeTransferModal();
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
  sendFileState.isPickingFile = true;  // Mark that file picker is about to open
  const peer = peers.get(peerId);
  if (peer && peer.dc && peer.dc.readyState === 'open') {
    try {
      peer.dc.send(JSON.stringify({ type: 'selecting-file' }));
    } catch(e){}
  }
  document.getElementById('file-input').click();
}

function startFileTransmission() {
  const file = sendFileState.file;
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
  
  // Show progress modal
  document.getElementById('transfer-title').textContent = 'Sending File';
  document.getElementById('transfer-peer-info').textContent = `To ${peerInfo ? peerInfo.name : 'Peer'}`;
  
  const badge = document.getElementById('connection-mode-badge');
  badge.textContent = 'Direct P2P (0 Internet Data)';
  badge.className = 'connection-mode-badge direct';

  document.getElementById('transfer-modal').classList.add('active');
  
  // Set low threshold event
  dc.onbufferedamountlow = () => {
    sendOrderedChunks();
  };
  
  sendNextChunks();
}

function sendNextChunks() {
  const file = sendFileState.file;
  const dc = sendFileState.activeChannel;
  
  if (!dc || dc.readyState !== 'open') return;
  
  const MAX_CONCURRENT_READS = 16;
  
  // Pause reading from disk if our memory queue is full or data channel buffer is saturated
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
    
    const reader = new FileReader();
    reader.onload = (event) => {
      const buffer = event.target.result;
      sendFileState.activeReads--;
      sendFileState.readQueue.set(currentIndex, buffer);
      sendOrderedChunks();
    };
    reader.onerror = (err) => {
      console.error('File read error:', err);
      sendFileState.activeReads--;
      cancelActiveTransfer();
    };
    reader.readAsArrayBuffer(slice);
  }
}

function sendOrderedChunks() {
  if (isSendingChunks) return;
  isSendingChunks = true;

  try {
    const dc = sendFileState.activeChannel;
    if (!dc || dc.readyState !== 'open') {
      return;
    }
    
    while (sendFileState.readQueue.has(sendFileState.sendIndex)) {
      if (dc.bufferedAmount >= BUFFER_THRESHOLD) {
        return;
      }
      
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
          setTimeout(closeTransferModal, 1500);
          return;
        }
      } catch (err) {
        console.error('DataChannel send error:', err);
        showToast('Failed to send chunk', 'danger');
        cancelActiveTransfer();
        return;
      }
    }
    
    sendNextChunks();
  } finally {
    isSendingChunks = false;
  }
}

// WebSocket Relay Transmission Logic
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
  
  // Cache encoded target peer ID bytes
  sendFileState.targetBytes = new TextEncoder().encode(sendFileState.targetPeerId); // 8 bytes
  
  const peerInfo = peers.get(sendFileState.targetPeerId);
  
  // Show progress modal
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
  
  const WS_CHUNK_SIZE = 262144; // 256KB chunks for WebSocket
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
    
    const WS_BUFFER_THRESHOLD = 1048576; // 1MB buffer threshold
    
    while (sendFileState.readQueue.has(sendFileState.sendIndex)) {
      if (socket.bufferedAmount >= WS_BUFFER_THRESHOLD) {
        // Schedule check shortly and pause sending
        isSendingWebSocket = false;
        setTimeout(sendOrderedChunksWebSocket, 2);
        return;
      }
      
      const index = sendFileState.sendIndex;
      const chunk = sendFileState.readQueue.get(index);
      sendFileState.readQueue.delete(index);
      
      try {
        // Construct binary payload: 8 bytes targetPeerId + chunk
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
          setTimeout(closeTransferModal, 1500);
          isSendingWebSocket = false;
          return;
        }
      } catch (err) {
        console.error('WebSocket send error:', err);
        showToast('Failed to send chunk', 'danger');
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

// WebSocket Relay Reception Logic
function handleRelayedWebSocketChunk(arrayBuffer) {
  processIncomingChunk(receiveFileState.senderPeerId, arrayBuffer);
}

function processIncomingChunk(peerId, data) {
  // Guard: ignore chunks if this state has been reset (e.g. after cancel)
  if (!receiveFileState.senderPeerId || receiveFileState.finalized) return;

  if (!receiveFileState.startTime) {
    receiveFileState.startTime = performance.now();
    receiveFileState.lastTime = receiveFileState.startTime;
    
    // Show transfer overlay as receiving
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
    // Feed chunk directly to Service Worker stream (using transferable arrayBuffer for zero-copy)
    navigator.serviceWorker.controller.postMessage({
      type: 'WRITE_CHUNK',
      streamId: receiveFileState.streamId,
      chunk: data
    }, [data]);
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
  // Guard against double-finalization (e.g. caused by a cancel signal arriving after completion)
  if (receiveFileState.finalized) return;
  receiveFileState.finalized = true;

  showToast('File received successfully!', 'success');
  addHistoryItem(receiveFileState.fileName, receiveFileState.fileSize, 'received', 'completed');
  
  if (receiveFileState.useStream && navigator.serviceWorker && navigator.serviceWorker.controller) {
    // Close the stream in Service Worker — browser will save it to disk
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
  
  // Cleanup after short delay
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
    
    // 1. Try to find the active candidate pair from the transport report
    let activePair = null;
    stats.forEach(report => {
      if (report.type === 'transport' && report.selectedCandidatePairId) {
        activePair = stats.get(report.selectedCandidatePairId);
      }
    });
    
    // 2. Fallback to scanning candidate-pairs directly
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

    // Determine if the connection is local
    const hasRelay = localType === 'relay' || remoteType === 'relay';
    const isLocal = !hasRelay && (
      localType === 'host' || 
      remoteType === 'host' || 
      isLocalIp(localIp) || 
      isLocalIp(remoteIp) ||
      (localIpAddress && !hasRelay) // If the phone is serving the local gateway, any non-relay direct WebRTC connection is local
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
  // Bump generation counter — all in-flight async callbacks will check this
  // and bail out if it no longer matches the generation they were started in.
  transferGeneration++;

  // Reset sender state
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

  // Cancel any active Service Worker stream for the receiver so it doesn't
  // linger and cause a stray partial-file download.
  if (receiveFileState.useStream && receiveFileState.streamId &&
      navigator.serviceWorker && navigator.serviceWorker.controller) {
    // Abort the stream (error state) so the browser discards the partial download and does NOT save a partial file.
    navigator.serviceWorker.controller.postMessage({
      type: 'CANCEL_STREAM',
      streamId: receiveFileState.streamId
    });
    // Immediately revoke the iframe so the browser stops the download.
    const iframe = document.getElementById('sw-download-iframe');
    if (iframe) {
      iframe.src = 'about:blank';
    }
  }

  // Reset receiver state
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

// Cancel transfers
function cancelActiveTransfer() {
  let hadActivity = false;

  // Sender: cancel if actively sending OR if waiting for accept (file chosen, no channel yet)
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
  
  // Receiver: cancel if waiting for or receiving data
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
    // Note: SW stream cleanup is handled inside resetAllTransferStates()
  }
  
  // resetAllTransferStates handles SW stream cleanup
  resetAllTransferStates();
  closeTransferModal();
  if (hadActivity) {
    showToast('Transfer cancelled', 'info');
  }
}

// UI Helper updates & Events
function addPeerCardToGrid(peerId, name) {
  const grid = document.getElementById('peer-grid');
  
  // Remove empty state if present
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
    // Insert before the peer-desc element
    const descEl = card.querySelector('.peer-desc');
    card.insertBefore(badgeEl, descEl);
  }
  
  const forceRelay = document.getElementById('relay-toggle').checked;
  
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
  console.log(`Handling WebRTC failure for peer: ${peerId}`);
  closePeerConnection(peerId);
}

function updatePeerCount() {
  document.getElementById('peer-count').textContent = `${peers.size} connected`;
}

function updateConnectionStatus(status, text) {
  const indicator = document.getElementById('connection-status');
  indicator.className = `connection-indicator ${status}`;
  indicator.querySelector('.status-text').textContent = text;
}

function updateQRTabSlider() {
  const activeTab = document.querySelector('.qr-tab.active');
  const slider = document.querySelector('.qr-tab-slider');
  if (activeTab && slider) {
    slider.style.left = `${activeTab.offsetLeft}px`;
    slider.style.width = `${activeTab.offsetWidth}px`;
  }
}

function setupUIEventListeners() {
  // Join Room Handler
  const joinBtn = document.getElementById('join-room-btn');
  const joinInput = document.getElementById('join-room-input');
  if (joinBtn && joinInput) {
    const performJoin = () => {
      const targetRoom = joinInput.value.trim().toLowerCase();
      if (!targetRoom) {
        showToast('Please enter a valid Room ID', 'danger');
        return;
      }
      if (targetRoom.length < 3) {
        showToast('Room ID must be at least 3 characters', 'danger');
        return;
      }
      window.location.hash = targetRoom;
      window.location.reload();
    };

    joinBtn.addEventListener('click', performJoin);
    joinInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') {
        performJoin();
      }
    });
  }

  // QR Mode tab switcher
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

// Request media permission temporarily to bypass WebRTC mDNS IP obfuscation
async function enableLocalIPs() {
  const btn = document.getElementById('optimize-p2p-btn');
  
  const isCapacitor = !!window.Capacitor || (window.location.hostname === 'localhost' && !window.location.port);
  const isNanoHTTPD = !isCapacitor && window.location.port === '8080' && window.location.hostname !== 'localhost';
  
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
    // Stop tracks immediately to release microphone
    stream.getTracks().forEach(track => track.stop());
    
    btn.textContent = 'Active';
    btn.className = 'btn btn-primary btn-sm';
    showToast('Local P2P optimized! Disabling mDNS obfuscation.', 'success');
    
    // Reconnect existing peer connections so they gather raw local IPs
    const oldPeers = Array.from(peers.keys());
    oldPeers.forEach(peerId => {
      removePeer(peerId);
    });
    
    // Trigger offer renegotiation on signaling channel to gather new candidates
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: 'ping' }));
    }
  } catch (err) {
    console.warn('Microphone permission denied, local IP gathering disabled:', err);
    btn.textContent = 'Failed';
    btn.className = 'btn btn-danger btn-sm';
    btn.disabled = false;
    showToast(`Failed: ${err.message}. Please click the lock/settings icon in the address bar and set Microphone to "Allow".`, 'danger');
  }
}

  // Relay toggle update connection badges
  document.getElementById('relay-toggle').addEventListener('change', () => {
    peers.forEach((peer, peerId) => {
      updatePeerConnectionBadge(peerId);
    });
  });

  // Optimize local P2P button click handler
  document.getElementById('optimize-p2p-btn').addEventListener('click', enableLocalIPs);

  // Copy Room Link
  document.getElementById('copy-room-btn').addEventListener('click', copyRoomLink);
  document.getElementById('invite-btn').addEventListener('click', copyRoomLink);
  
  // Clear History
  document.getElementById('clear-history-btn').addEventListener('click', clearHistory);
  
  // File Input Handler
  document.getElementById('file-input').addEventListener('change', async (e) => {
    sendFileState.isPickingFile = false;  // File picker closed
    const file = e.target.files[0];
    if (!file || !sendFileState.targetPeerId) return;

    sendFileState.file = file;
    
    const forceRelay = document.getElementById('relay-toggle').checked;
    const existingPeer = peers.get(sendFileState.targetPeerId);

    // Only tear down and reconnect if the WebRTC Data Channel is not currently open/active
    // OR if we are forcing relay mode.
    if ((!existingPeer || !existingPeer.dc || existingPeer.dc.readyState !== 'open') && !forceRelay) {
      console.log('Direct WebRTC Data Channel not open. Re-initiating peer connection...');
      closePeerConnection(sendFileState.targetPeerId);
      initiatePeerConnection(sendFileState.targetPeerId);
    } else {
      console.log('Direct WebRTC Data Channel is already open and active. Reusing existing connection.');
    }
    
    const peer = peers.get(sendFileState.targetPeerId);

    // --- Show the "waiting" UI immediately so user sees feedback ---
    document.getElementById('transfer-title').textContent = 'Waiting for Accept';
    document.getElementById('transfer-peer-info').textContent = `Waiting for ${peer ? peer.name : 'Peer'} to accept...`;
    document.getElementById('transfer-speed').textContent = '-';
    document.getElementById('time-remaining').textContent = '-';
    document.getElementById('progress-bar').style.width = '0%';
    document.getElementById('progress-percent').textContent = '0%';
    document.getElementById('transferred-bytes').textContent = `File: ${file.name} (${formatBytes(file.size)})`;
    document.getElementById('transfer-modal').classList.add('active');

    // --- Wait up to 8s for the data channel to open.
    //     The WebView may have been paused during file picking causing the
    //     signaling WebSocket to drop and WebRTC to need re-establishing.
    //     8 seconds gives time for: signaling reconnect + WebRTC re-negotiation.
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
      console.log('DataChannel wait result:', dcReady ? 'open' : 'timed out — using relay');
    }

    // Re-read peer after await (it may have reconnected and re-established)
    const freshPeer = peers.get(sendFileState.targetPeerId);

    const badge = document.getElementById('connection-mode-badge');

    if (freshPeer && freshPeer.dc && freshPeer.dc.readyState === 'open' && !forceRelay) {
      sendFileState.useWebSocketRelay = false;
      badge.textContent = 'Will use Direct P2P (0 Internet Data)';
      badge.className = 'connection-mode-badge direct';

      // Send meta information to receiver via WebRTC data channel
      freshPeer.dc.send(JSON.stringify({
        type: 'meta',
        name: file.name,
        size: file.size
      }));
    } else {
      // Fallback: Send meta over WebSocket Signaling channel
      sendFileState.useWebSocketRelay = true;
      badge.textContent = 'Will use WebSocket Relay (Uses Internet Data)';
      badge.className = 'connection-mode-badge relay';
      showToast('P2P not ready — relaying via WebSocket...', 'info');
      console.log('Sending ws-meta signal to target peer:', sendFileState.targetPeerId);
      sendSignal(sendFileState.targetPeerId, {
        type: 'ws-meta',
        name: file.name,
        size: file.size
      });
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
  const targetUrl = `https://ajshare.pages.dev/#${roomId}`;
  navigator.clipboard.writeText(targetUrl).then(() => {
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

// QR Code generation
let qr = null;
function generateQRCode() {
  const canvas = document.getElementById('qr-code-canvas');
  if (!canvas) return;

  const targetUrl = `https://ajshare.pages.dev/#${roomId}`;
  
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

// Transfer History Management
let transferHistory = [];

function loadHistory() {
  try {
    const data = localStorage.getItem('ajshare_history');
    if (data) {
      transferHistory = JSON.parse(data);
    }
  } catch(e) {
    console.error('Error loading history:', e);
  }
  renderHistory();
}

function saveHistory() {
  try {
    localStorage.setItem('ajshare_history', JSON.stringify(transferHistory));
  } catch(e) {
    console.error('Error saving history:', e);
  }
}

function addHistoryItem(name, size, type, status) {
  const item = {
    id: Math.random().toString(36).substring(2, 9),
    name: name,
    size: size,
    type: type, // 'sent' or 'received'
    status: status, // 'completed' or 'cancelled'
    timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  };
  transferHistory.unshift(item); // Add to front of history
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
  
  // Clear previous items
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
    const typeClass = item.type;
    
    row.innerHTML = `
      <div class="history-left">
        <div class="history-icon-badge ${typeClass}">${iconChar}</div>
        <div class="history-details">
          <h4 class="history-file-name" style="cursor: pointer; text-decoration: underline; text-underline-offset: 2px;">${escapeHTML(item.name)}</h4>
          <p>${formatBytes(item.size)} • ${item.timestamp}</p>
        </div>
      </div>
      <div class="history-right">
        <span class="history-status ${item.status}">${item.status}</span>
      </div>
    `;

    const fileNameEl = row.querySelector('.history-file-name');
    fileNameEl.addEventListener('click', () => {
      if (item.status === 'completed') {
        if (item.type === 'received') {
          showToast(`Look in your browser's "Downloads" folder for this file.`, 'info');
        } else {
          showToast(`This file was sent from your local device.`, 'info');
        }
      }
    });

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
  return str.replace(/[&<>'"]/g, 
    tag => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;'
    }[tag] || tag)
  );
}
