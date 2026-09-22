// Minimal service worker — only needed so the browser allows "Add to Home Screen".
// It does not cache the radio itself, so you always get the newest version live.
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => { self.clients.claim(); });
self.addEventListener('fetch', (e) => {
  e.respondWith(fetch(e.request).catch(() => new Response('Offline', { status: 503 })));
});
