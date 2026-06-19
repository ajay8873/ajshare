// sw.js - Service Worker for streaming WebRTC downloads directly to disk

// Map to store active stream controllers by streamId
const activeStreams = new Map();

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('message', (event) => {
  const data = event.data;
  if (!data) return;

  const { type, streamId, chunk, name, size } = data;

  if (type === 'CREATE_STREAM') {
    let controller;
    const stream = new ReadableStream({
      start(c) {
        controller = c;
      },
      cancel() {
        activeStreams.delete(streamId);
      }
    });

    activeStreams.set(streamId, {
      stream,
      controller,
      name,
      size,
      received: 0
    });
    
    // Acknowledge stream creation back to client script
    if (event.ports && event.ports[0]) {
      event.ports[0].postMessage({ status: 'OK' });
    }
  } else if (type === 'WRITE_CHUNK') {
    const streamInfo = activeStreams.get(streamId);
    if (streamInfo && streamInfo.controller) {
      try {
        // Enqueue the binary chunk directly into the browser's response stream
        streamInfo.controller.enqueue(new Uint8Array(chunk));
        streamInfo.received += chunk.byteLength;
      } catch (err) {
        console.error('Error enqueuing chunk to stream:', err);
      }
    }
  } else if (type === 'CLOSE_STREAM') {
    const streamInfo = activeStreams.get(streamId);
    if (streamInfo && streamInfo.controller) {
      try {
        streamInfo.controller.close();
      } catch (err) {}
      activeStreams.delete(streamId);
    }
  } else if (type === 'CANCEL_STREAM') {
    const streamInfo = activeStreams.get(streamId);
    if (streamInfo && streamInfo.controller) {
      try {
        streamInfo.controller.error(new Error('Stream cancelled by user'));
      } catch (err) {}
      activeStreams.delete(streamId);
    }
  }
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Intercept downloads routed through /download-stream/
  if (url.pathname.startsWith('/download-stream/')) {
    const streamId = url.pathname.split('/').pop();
    const streamInfo = activeStreams.get(streamId);

    if (streamInfo) {
      const headers = new Headers({
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${encodeURIComponent(streamInfo.name)}"`,
        'Content-Length': streamInfo.size.toString(),
        'X-Content-Type-Options': 'nosniff'
      });

      // Respond with the ReadableStream - browser streams this straight to the disk
      event.respondWith(
        new Response(streamInfo.stream, {
          headers,
          status: 200,
          statusText: 'OK'
        })
      );
    } else {
      event.respondWith(
        new Response('Download link expired or invalid.', { status: 404 })
      );
    }
  }
});
