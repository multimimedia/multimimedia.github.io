/* Hemeroteca WebRTC HTTPS P2P Service Worker (sw.js) */
const SW_VERSION = 'hmr-webrtc-sw-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

const HMR_TUNNEL_PATHS = [
  '/__hmr_entry',
  '/login',
  '/register',
  '/logout',
  '/channels',
  '/browse',
  '/video',
  '/channel/',
  '/api/',
  '/admin/',
  '/webdav',
  '/s/',
  '/notifications/',
  '/favicon.',
  '/apple-touch-icon',
  '/splash-emblem.webp',
  '/ping'
];

function shouldTunnelPath(urlObj, request) {
  if (urlObj.origin !== self.location.origin) return false;
  const p = urlObj.pathname;
  if (p === '/sw.js' || p === '/robots.txt' || p === '/sitemap.xml') return false;
  if (urlObj.searchParams.has('__hmr_outer')) return false;
  if (urlObj.searchParams.has('__hmr_tunnel')) return true;
  for (let i = 0; i < HMR_TUNNEL_PATHS.length; i++) {
    const prefix = HMR_TUNNEL_PATHS[i];
    if (p === prefix || p.startsWith(prefix)) return true;
  }
  return false;
}

async function findControllerClient(sourceClientId) {
  const allClients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
  // Prefer top-level window frame (frameType === 'top-level')
  for (const c of allClients) {
    if (c.frameType === 'top-level') return c;
  }
  return allClients[0] || null;
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  const urlObj = new URL(req.url);
  if (!shouldTunnelPath(urlObj, req)) {
    return;
  }

  event.respondWith((async () => {
    const controller = await findControllerClient(event.clientId);
    if (!controller) {
      return new Response('Tunel WebRTC P2P no inicializado.', { status: 503 });
    }

    let cleanPath = urlObj.pathname;
    if (cleanPath === '/__hmr_entry') {
      cleanPath = '/';
    }
    const cleanParams = new URLSearchParams(urlObj.search);
    cleanParams.delete('__hmr_tunnel');
    const qs = cleanParams.toString();
    const targetPath = cleanPath + (qs ? '?' + qs : '');

    const headersList = [];
    req.headers.forEach((value, name) => {
      headersList.push({ name, value });
    });

    let bodyBytes = null;
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      try {
        const ab = await req.arrayBuffer();
        if (ab && ab.byteLength > 0) {
          bodyBytes = new Uint8Array(ab);
        }
      } catch (_e) {}
    }

    return new Promise((resolve) => {
      const msgChannel = new MessageChannel();
      let streamController = null;
      let resolvedMeta = false;

      const bodyStream = new ReadableStream({
        start(ctrl) {
          streamController = ctrl;
        },
        cancel() {
          try {
            msgChannel.port1.postMessage({ type: 'ABORT' });
          } catch (_e) {}
        }
      });

      const timeoutId = setTimeout(() => {
        if (!resolvedMeta) {
          resolvedMeta = true;
          resolve(new Response('Tiempo de espera agotado en tunel WebRTC P2P.', { status: 504 }));
        }
      }, 32000);

      msgChannel.port1.onmessage = (e) => {
        const data = e.data || {};
        if (data.type === 'META') {
          clearTimeout(timeoutId);
          resolvedMeta = true;
          const respHeaders = new Headers();
          (data.headers || []).forEach((h) => {
            const lower = (h.name || '').toLowerCase();
            if (lower && lower !== 'set-cookie' && lower !== 'content-length' && lower !== 'transfer-encoding') {
              try {
                respHeaders.append(h.name, h.value);
              } catch (_e) {}
            }
          });
          const status = data.statusCode || 200;
          const noBodyStatus = status === 204 || status === 304 || req.method === 'HEAD';
          resolve(new Response(noBodyStatus ? null : bodyStream, {
            status: status,
            statusText: data.reasonPhrase || 'OK',
            headers: respHeaders
          }));
          if (noBodyStatus && streamController) {
            try { streamController.close(); } catch (_e) {}
          }
        } else if (data.type === 'CHUNK') {
          if (streamController && data.chunk) {
            try {
              streamController.enqueue(new Uint8Array(data.chunk));
            } catch (_e) {}
          }
        } else if (data.type === 'END') {
          if (!resolvedMeta) {
            clearTimeout(timeoutId);
            resolvedMeta = true;
            resolve(new Response('', { status: 200 }));
          }
          if (streamController) {
            try { streamController.close(); } catch (_e) {}
          }
          try { msgChannel.port1.close(); } catch (_e) {}
        } else if (data.type === 'ERROR') {
          clearTimeout(timeoutId);
          if (!resolvedMeta) {
            resolvedMeta = true;
            resolve(new Response(data.message || 'Error en tunel WebRTC', { status: 502 }));
          } else if (streamController) {
            try { streamController.error(new Error(data.message || 'Stream error')); } catch (_e) {}
          }
          try { msgChannel.port1.close(); } catch (_e) {}
        }
      };

      const transferList = [msgChannel.port2];
      let bodyBuffer = null;
      if (bodyBytes) {
        bodyBuffer = bodyBytes.buffer;
        transferList.push(bodyBuffer);
      }

      controller.postMessage({
        type: 'HMR_SW_FETCH',
        method: req.method,
        path: targetPath,
        headers: headersList,
        bodyBuffer: bodyBuffer
      }, transferList);
    });
  })());
});
