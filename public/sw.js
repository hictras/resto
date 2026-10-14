 self.addEventListener('push', function (event) {
    let data = { title: 'Restaurant Update', body: 'You have a new notification.', url: null };
    try { data = Object.assign(data, event.data.json()); } catch (e) {}

    event.waitUntil(
        self.registration.showNotification(data.title, {
            body: data.body,
            icon: '/icon-192.png',
            badge: '/icon-192.png',
            data: { url: data.url }
        })
    );
});

self.addEventListener('notificationclick', function (event) {
    event.notification.close();
    const targetUrl = (event.notification.data && event.notification.data.url) || null;

    event.waitUntil((async () => {
        const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
        // Prefer an already-open menu page
        for (const c of allClients) {
            if (c.url.includes('/menu.html')) {
                await c.focus();
                if (targetUrl && !c.url.endsWith(targetUrl)) await c.navigate(targetUrl);
                return;
            }
        }
        // Otherwise open a new window
        return clients.openWindow(targetUrl || '/');
    })());
});