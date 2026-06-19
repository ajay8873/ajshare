// worker.js
// Cloudflare Worker Signaling Server for AJShare (WebRTC file sharing)

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Serve WebSockets at /ws
    if (url.pathname === '/ws') {
      const room = url.searchParams.get('room');
      if (!room) {
        return new Response('Room parameter is required', { status: 400 });
      }

      // Route request to the Room Durable Object
      const id = env.ROOM_DO.idFromName(room);
      const roomObject = env.ROOM_DO.get(id);
      return roomObject.fetch(request);
    }

    // Default status route
    return new Response('AJShare Signaling Server is running.', {
      status: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'text/plain'
      }
    });
  }
};

// Durable Object class for managing a single peer room
export class RoomDO {
  constructor(state, env) {
    this.state = state;
    // Map of client ID -> WebSocket connection
    this.sessions = new Map();
  }

  async fetch(request) {
    // Expect a WebSocket upgrade
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected Upgrade: websocket', { status: 426 });
    }

    const [clientSocket, serverSocket] = new WebSocketPair();

    // Accept server-side socket
    await this.handleSession(serverSocket);

    return new Response(null, {
      status: 101,
      webSocket: clientSocket
    });
  }

  async handleSession(ws) {
    ws.accept();

    // Generate a unique client ID for this connection
    const clientId = crypto.randomUUID().slice(0, 8);
    this.sessions.set(clientId, ws);

    // Send the client their ID and tell them they successfully joined
    ws.send(JSON.stringify({
      type: 'welcome',
      clientId: clientId,
      peers: Array.from(this.sessions.keys()).filter(id => id !== clientId)
    }));

    // Broadcast "peer-joined" to all other clients in the room
    this.broadcast(JSON.stringify({
      type: 'peer-joined',
      peerId: clientId
    }), clientId);

    ws.addEventListener('message', async (msg) => {
      try {
        const data = JSON.parse(msg.data);

        switch (data.type) {
          case 'signal':
            // Relay WebRTC signal to a specific target peer
            if (data.target && this.sessions.has(data.target)) {
              const targetSocket = this.sessions.get(data.target);
              targetSocket.send(JSON.stringify({
                type: 'signal',
                sender: clientId,
                signal: data.signal
              }));
            }
            break;

          case 'ping':
            // Keep-alive ping
            ws.send(JSON.stringify({ type: 'pong' }));
            break;
        }
      } catch (err) {
        console.error('Error handling WebSocket message:', err);
      }
    });

    // Handle closure and error
    const closeHandler = () => {
      if (this.sessions.has(clientId)) {
        this.sessions.delete(clientId);
        // Notify other peers in the room
        this.broadcast(JSON.stringify({
          type: 'peer-left',
          peerId: clientId
        }));
      }
    };

    ws.addEventListener('close', closeHandler);
    ws.addEventListener('error', closeHandler);
  }

  // Helper to broadcast to all clients in the room, optionally excluding sender
  broadcast(message, excludeClientId = null) {
    for (const [clientId, ws] of this.sessions.entries()) {
      if (clientId !== excludeClientId) {
        try {
          ws.send(message);
        } catch (err) {
          console.error(`Error sending to client ${clientId}:`, err);
        }
      }
    }
  }
}
