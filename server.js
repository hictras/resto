const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const webpush = require('web-push');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Data dirs ----------
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
const restaurantsDir = path.join(__dirname, 'restaurants');
if (!fs.existsSync(restaurantsDir)) fs.mkdirSync(restaurantsDir, { recursive: true });

// ---------- VAPID keys ----------
const vapidPath = path.join(dataDir, 'vapid.json');
let vapidKeys;
if (fs.existsSync(vapidPath)) {
    vapidKeys = JSON.parse(fs.readFileSync(vapidPath, 'utf8'));
} else {
    vapidKeys = webpush.generateVAPIDKeys();
    fs.writeFileSync(vapidPath, JSON.stringify(vapidKeys, null, 2));
}
webpush.setVapidDetails('mailto:admin@lastballoon.net', vapidKeys.publicKey, vapidKeys.privateKey);

app.get('/api/vapid-public-key', (req, res) => res.json({ publicKey: vapidKeys.publicKey }));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'customer.html')));
app.get('/menu.html', (req, res) => res.sendFile(path.join(__dirname, 'public', 'menu.html')));

// ---------- Database ----------
const dbPath = path.join(dataDir, 'restaurant.db');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

db.exec(`
    CREATE TABLE IF NOT EXISTS tables_state (
        restaurant TEXT, table_number TEXT, is_open INTEGER, can_order INTEGER, status TEXT,
        PRIMARY KEY (restaurant, table_number)
    );
    CREATE TABLE IF NOT EXISTS table_requests (
        restaurant TEXT, table_number TEXT, requested INTEGER,
        PRIMARY KEY (restaurant, table_number)
    );
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, items TEXT,
        total REAL, kitchen_status TEXT, bill_status TEXT, date TEXT, timestamp TEXT, created_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS paid_history (
        order_id TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, items TEXT,
        total REAL, kitchen_status TEXT, bill_status TEXT, date TEXT, timestamp TEXT, created_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT,
        start_time INTEGER, lifetime_minutes INTEGER DEFAULT 15
    );
    CREATE TABLE IF NOT EXISTS push_subs (
        endpoint TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, sub TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_orders_resto_table ON orders(restaurant, table_number);
    CREATE INDEX IF NOT EXISTS idx_paid_resto_date ON paid_history(restaurant, date);
    CREATE INDEX IF NOT EXISTS idx_sessions_resto_table ON sessions(restaurant, table_number);
    CREATE INDEX IF NOT EXISTS idx_push_resto_table ON push_subs(restaurant, table_number);
`);

// ---------- Migrations ----------
function columnExists(table, column) {
    return db.prepare(`PRAGMA table_info(${table})`).all().some(r => r.name === column);
}
if (!columnExists('orders', 'created_at')) {
    db.exec('ALTER TABLE orders ADD COLUMN created_at INTEGER');
    db.prepare('UPDATE orders SET created_at = ? WHERE created_at IS NULL').run(Date.now());
}
if (!columnExists('paid_history', 'created_at')) {
    db.exec('ALTER TABLE paid_history ADD COLUMN created_at INTEGER');
    db.prepare('UPDATE paid_history SET created_at = ? WHERE created_at IS NULL').run(Date.now());
}

// ---------- Push subscription helpers (persisted) ----------
function savePushSub(resto, table, sub) {
    db.prepare(`INSERT OR REPLACE INTO push_subs (endpoint, restaurant, table_number, sub) VALUES (?, ?, ?, ?)`)
        .run(sub.endpoint, resto, String(table), JSON.stringify(sub));
}
function getPushSubs(resto, table) {
    return db.prepare('SELECT sub FROM push_subs WHERE restaurant = ? AND table_number = ?')
        .all(resto, String(table)).map(r => JSON.parse(r.sub));
}
function removePushSub(endpoint) {
    db.prepare('DELETE FROM push_subs WHERE endpoint = ?').run(endpoint);
}

app.post('/api/subscribe-push', (req, res) => {
    const { restaurant, tableNumber, subscription } = req.body;
    if (!restaurant || !tableNumber || !subscription || !subscription.endpoint) {
        return res.status(400).json({ success: false, message: 'Missing fields' });
    }
    savePushSub(path.basename(String(restaurant)), String(tableNumber), subscription);
    res.status(201).json({ success: true });
});

// ---------- Socket state ----------
const tableSockets = {}; // sessionKey -> Set<socketId>

// Cache app state to avoid recomputing on every event
let cachedState = null;
let cacheDirty = true;
function invalidateCache() { cacheDirty = true; }

function buildAppState() {
    const tables = {};
    db.prepare('SELECT * FROM tables_state').all().forEach(r => {
        if (!tables[r.restaurant]) tables[r.restaurant] = {};
        tables[r.restaurant][r.table_number] = {
            isOpen: !!r.is_open, canOrder: !!r.can_order, status: r.status
        };
    });

    // ---- Active sessions: only report tables that currently have a live socket ----
    // This prevents "1 active" phantom badges when a session row exists but the
    // customer has closed their browser tab (or never connected yet).
    const activeSessions = {};
    const now = Date.now();
    db.prepare('SELECT restaurant, table_number, start_time, lifetime_minutes FROM sessions').all().forEach(s => {
        const lifetimeMs = (s.lifetime_minutes || 15) * 60 * 1000;
        if (now - s.start_time <= lifetimeMs) {
            const sessionKey = `${s.restaurant}_${s.table_number}`;
            const socketCount = (tableSockets[sessionKey] && tableSockets[sessionKey].size) || 0;
            if (socketCount > 0) {
                if (!activeSessions[s.restaurant]) activeSessions[s.restaurant] = {};
                activeSessions[s.restaurant][s.table_number] = socketCount;
            }
        }
    });

    const tableRequests = {};
    db.prepare('SELECT * FROM table_requests WHERE requested = 1').all().forEach(r => {
        if (!tableRequests[r.restaurant]) tableRequests[r.restaurant] = {};
        tableRequests[r.restaurant][r.table_number] = true;
    });

    const orders = db.prepare('SELECT * FROM orders').all().map(o => ({
        orderId: o.order_id, restaurant: o.restaurant, tableNumber: o.table_number,
        items: JSON.parse(o.items), total: o.total, kitchenStatus: o.kitchen_status,
        billStatus: o.bill_status, date: o.date, timestamp: o.timestamp, createdAt: o.created_at
    }));

    return { tables, tableRequests, orders, activeSessions };
}

function getAppState() {
    if (cacheDirty || !cachedState) {
        cachedState = buildAppState();
        cacheDirty = false;
    }
    return cachedState;
}

function broadcastState() {
    invalidateCache();
    io.emit('state_update', getAppState());
}

function isSessionValid(session) {
    if (!session) return false;
    const lifetimeMs = (session.lifetime_minutes || 15) * 60 * 1000;
    return (Date.now() - session.start_time <= lifetimeMs);
}

// ---------- Session cleanup ----------
setInterval(() => {
    const now = Date.now();
    const rows = db.prepare('SELECT token, start_time, lifetime_minutes FROM sessions').all();
    let changed = false;
    rows.forEach(s => {
        const lifetimeMs = (s.lifetime_minutes || 15) * 60 * 1000;
        if (now - s.start_time > lifetimeMs) {
            db.prepare('DELETE FROM sessions WHERE token = ?').run(s.token);
            changed = true;
        }
    });
    if (changed) broadcastState();
}, 60_000);

// ---------- API ----------
app.get('/api/verify-token', (req, res) => {
    const { token } = req.query;
    if (!token) return res.status(400).json({ success: false, message: 'Missing token' });
    const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
    if (!isSessionValid(session)) return res.status(401).json({ success: false, message: 'Invalid or expired token' });
    const lifetimeMinutes = session.lifetime_minutes || 15;
    res.json({
        success: true, restaurant: session.restaurant, tableNumber: session.table_number,
        lifetimeMinutes, expiresAt: session.start_time + lifetimeMinutes * 60 * 1000,
        sessionStart: session.start_time
    });
});

app.post('/api/create-session', (req, res) => {
    const { restaurant, tableNumber, lifetime } = req.body;
    if (!restaurant || !tableNumber) {
        return res.status(400).json({ success: false, message: 'Restaurant and table number required' });
    }
    try {
        const resto = path.basename(String(restaurant));
        const tKey = String(tableNumber);
        const lifetimeMinutes = parseInt(lifetime) || 15;
        const now = Date.now();

        // Delete any existing sessions for this table to avoid orphaning
        db.prepare('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?').run(resto, tKey);

        const token = 'tok_' + uuidv4().replace(/-/g, '') + now.toString(36);
        db.prepare(`INSERT INTO sessions (token, restaurant, table_number, start_time, lifetime_minutes) VALUES (?, ?, ?, ?, ?)`)
            .run(token, resto, tKey, now, lifetimeMinutes);

        broadcastState();
        res.json({ success: true, token, expiresAt: now + lifetimeMinutes * 60 * 1000, lifetimeMinutes });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: 'Internal server error' });
    }
});

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
    socket.emit('state_update', getAppState());

    socket.on('register_session', ({ token }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
        if (!isSessionValid(session)) return cb({ success: false, message: 'Invalid token' });

        const resto = session.restaurant;
        const tKey = session.table_number;
        const sessionKey = `${resto}_${tKey}`;

        if (!tableSockets[sessionKey]) tableSockets[sessionKey] = new Set();
        tableSockets[sessionKey].add(socket.id);
        socket.tableSessionKey = sessionKey;
        socket.tableToken = token;
        socket.join(`table_${resto}_${tKey}`);

        broadcastState();
        cb({ success: true, restaurant: resto, tableNumber: tKey });
    });

    socket.on('update_cart', ({ token, cart }) => {
        const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
        if (!isSessionValid(session)) return;
        socket.to(`table_${session.restaurant}_${session.table_number}`).emit('cart_synced', { cart });
    });

    socket.on('disconnect', () => {
        const sessionKey = socket.tableSessionKey;
        const token = socket.tableToken;
        if (sessionKey && tableSockets[sessionKey]) {
            tableSockets[sessionKey].delete(socket.id);
            if (tableSockets[sessionKey].size === 0) {
                delete tableSockets[sessionKey];
                const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
                if (session && !isSessionValid(session)) {
                    db.prepare('DELETE FROM sessions WHERE token = ?').run(token);
                }
            }
            broadcastState();
        }
    });

    socket.on('toggle_table', ({ restaurant, tableNum }) => {
        const resto = restaurant || 'Default';
        const tKey = String(tableNum);
        const current = db.prepare('SELECT is_open FROM tables_state WHERE restaurant = ? AND table_number = ?').get(resto, tKey);
        if (!current) return;
        const newOpen = current.is_open ? 0 : 1;
        if (newOpen === 0) {
            db.prepare('UPDATE tables_state SET is_open = 0, can_order = 0 WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
            db.prepare('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        } else {
            db.prepare('UPDATE tables_state SET is_open = 1 WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        }
        broadcastState();
    });

    socket.on('toggle_order_permission', ({ restaurant, tableNum }) => {
        const resto = restaurant || 'Default';
        const tKey = String(tableNum);
        const current = db.prepare('SELECT can_order FROM tables_state WHERE restaurant = ? AND table_number = ?').get(resto, tKey);
        if (!current) return;
        const newCanOrder = current.can_order ? 0 : 1;
        db.prepare('UPDATE tables_state SET can_order = ? WHERE restaurant = ? AND table_number = ?').run(newCanOrder, resto, tKey);
        if (newCanOrder) {
            db.prepare('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        }
        broadcastState();
    });

    socket.on('table_request', ({ token }) => {
        const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
        if (!isSessionValid(session)) return;
        db.prepare(`INSERT INTO table_requests (restaurant, table_number, requested) VALUES (?, ?, 1)
                    ON CONFLICT(restaurant, table_number) DO UPDATE SET requested = 1`)
            .run(session.restaurant, session.table_number);
        broadcastState();
    });

    socket.on('place_order', ({ items, total, token }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
            if (!isSessionValid(session)) return cb({ success: false, message: 'Session expired or unauthorized.' });

            const resto = session.restaurant;
            const tKey = session.table_number;
            const tableState = db.prepare('SELECT * FROM tables_state WHERE restaurant = ? AND table_number = ?').get(resto, tKey);
            if (!tableState || !tableState.is_open || !tableState.can_order) {
                return cb({ success: false, message: 'Order submission is locked by the manager.' });
            }

            const orderId = uuidv4();
            const date = new Date().toISOString().split('T')[0];
            const timestamp = new Date().toLocaleTimeString();
            const createdAt = Date.now();
            const formattedItems = JSON.stringify(items.map(i => ({ ...i, checks: [false, false] })));

            db.transaction(() => {
                db.prepare(`INSERT INTO orders (order_id, restaurant, table_number, items, total, kitchen_status, bill_status, date, timestamp, created_at)
                            VALUES (?, ?, ?, ?, ?, 'Received', 'Unpaid', ?, ?, ?)`)
                    .run(orderId, resto, tKey, formattedItems, total, date, timestamp, createdAt);
                db.prepare('UPDATE tables_state SET can_order = 0 WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
            })();

            io.to(`table_${resto}_${tKey}`).emit('order_placed_success');
            broadcastState();
            cb({ success: true, orderId });
        } catch (e) {
            console.error('place_order failed:', e);
            cb({ success: false, message: 'Server error.' });
        }
    });

    socket.on('update_kitchen_status', ({ orderId, kitchenStatus }) => {
        db.prepare('UPDATE orders SET kitchen_status = ? WHERE order_id = ?').run(kitchenStatus, orderId);
        const order = db.prepare('SELECT * FROM orders WHERE order_id = ?').get(orderId);
        if (order) {
            const subs = getPushSubs(order.restaurant, order.table_number);
            const payload = JSON.stringify({
                title: kitchenStatus === 'Sent to Table' ? 'Order Delivered! 🍽️' : 'Kitchen Update 🍳',
                body: kitchenStatus === 'Sent to Table'
                    ? 'Your food has been delivered to your table!'
                    : `Your order status is now: ${kitchenStatus}`,
                url: '/menu.html'
            });
            subs.forEach(sub => {
                webpush.sendNotification(sub, payload).catch(err => {
                    if (err.statusCode === 404 || err.statusCode === 410) removePushSub(sub.endpoint);
                });
            });
        }
        broadcastState();
    });

    socket.on('toggle_item_check', ({ orderId, itemIndex, checkIndex }) => {
        const orderRow = db.prepare('SELECT items FROM orders WHERE order_id = ?').get(orderId);
        if (!orderRow) return;
        const items = JSON.parse(orderRow.items);
        if (!items[itemIndex]) return;
        if (!items[itemIndex].checks) items[itemIndex].checks = [false, false];
        items[itemIndex].checks[checkIndex] = !items[itemIndex].checks[checkIndex];

        const anyCook = items.some(i => i.checks && i.checks[0] && !i.checks[1]);
        const allReady = items.every(i => i.checks && i.checks[1]);
        const statusUpdate = allReady ? 'Ready' : (anyCook ? 'Cooking' : 'Received');

        db.prepare('UPDATE orders SET items = ?, kitchen_status = ? WHERE order_id = ?')
            .run(JSON.stringify(items), statusUpdate, orderId);
        broadcastState();
    });

    socket.on('update_bill_status', ({ orderId, billStatus }) => {
        const order = db.prepare('SELECT * FROM orders WHERE order_id = ?').get(orderId);
        if (!order) return;

        if (billStatus === 'Paid') {
            db.transaction(() => {
                db.prepare(`INSERT OR REPLACE INTO paid_history
                    (order_id, restaurant, table_number, items, total, kitchen_status, bill_status, date, timestamp, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
                    .run(order.order_id, order.restaurant, order.table_number, order.items,
                        order.total, order.kitchen_status, 'Paid', order.date, order.timestamp,
                        order.created_at || Date.now());
                db.prepare('DELETE FROM orders WHERE order_id = ?').run(orderId);
            })();

            const remainingUnpaid = db.prepare(
                `SELECT COUNT(*) AS c FROM orders WHERE restaurant = ? AND table_number = ? AND bill_status != 'Paid'`
            ).get(order.restaurant, order.table_number).c;

            if (remainingUnpaid === 0) {
                db.prepare('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?')
                    .run(order.restaurant, order.table_number);
                io.to(`table_${order.restaurant}_${order.table_number}`).emit('session_ended', { reason: 'paid' });
            }
        } else {
            db.prepare('UPDATE orders SET bill_status = ? WHERE order_id = ?').run(billStatus, orderId);
        }
        broadcastState();
    });

    socket.on('clear_bill', ({ orderId }) => {
        db.prepare('DELETE FROM orders WHERE order_id = ?').run(orderId);
        broadcastState();
    });
});

// ---------- REST APIs ----------
app.get('/api/restaurants', (req, res) => {
    try {
        let files = fs.readdirSync(restaurantsDir).filter(f => f.endsWith('.json'));
        if (files.length === 0) {
            fs.writeFileSync(path.join(restaurantsDir, 'Default.json'), JSON.stringify([], null, 4));
            files = ['Default.json'];
        }
        res.json(files.map(f => path.basename(f, '.json')));
    } catch (e) {
        res.status(500).json({ error: 'Unable to scan restaurants directory' });
    }
});

app.get('/api/qr-links', (req, res) => {
    try {
        const targetResto = req.query.restaurant;
        const lifetimeMinutes = parseInt(req.query.lifetime) || 15;
        let files = fs.readdirSync(restaurantsDir).filter(f => f.endsWith('.json'));
        let restaurants = files.map(f => path.basename(f, '.json'));
        if (targetResto) restaurants = restaurants.filter(r => r === targetResto);

        const allLinks = [];
        restaurants.forEach(resto => {
            const restoTables = db.prepare('SELECT table_number FROM tables_state WHERE restaurant = ?').all(resto);
            restoTables.forEach(t => {
                const tableNumStr = String(t.table_number);
                // Fresh session per QR generation
                db.prepare('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?').run(resto, tableNumStr);
                const token = 'tok_' + uuidv4().replace(/-/g, '') + Date.now().toString(36);
                db.prepare(`INSERT INTO sessions (token, restaurant, table_number, start_time, lifetime_minutes) VALUES (?, ?, ?, ?, ?)`)
                    .run(token, resto, tableNumStr, Date.now(), lifetimeMinutes);

                // Encode with padding preserved (reverse after base64)
                const encodedTbl = Buffer.from(tableNumStr).toString('base64').split('').reverse().join('');
                const encodedResto = Buffer.from(resto).toString('base64').split('').reverse().join('');

                allLinks.push({
                    restaurant: resto, tableNumber: t.table_number,
                    encodedUrl: `/customer.html?tbl=${encodeURIComponent(encodedTbl)}&rst=${encodeURIComponent(encodedResto)}&lifetime=${lifetimeMinutes}`
                });
            });
        });
        broadcastState();
        res.json(allLinks);
    } catch (e) {
        console.error('QR Links Error:', e);
        res.status(500).json({ error: 'Failed to generate QR links', details: e.message });
    }
});

app.post('/api/tables/:restaurantName', (req, res) => {
    const safeName = path.basename(req.params.restaurantName);
    const { tableNumber } = req.body;
    if (!tableNumber) return res.status(400).json({ success: false, message: 'Table number required' });

    db.prepare(`INSERT INTO tables_state (restaurant, table_number, is_open, can_order, status)
                VALUES (?, ?, 0, 0, 'Valid')
                ON CONFLICT(restaurant, table_number) DO NOTHING`)
        .run(safeName, String(tableNumber));
    broadcastState();
    res.json({ success: true });
});

app.delete('/api/tables/:restaurantName/:tableNumber', (req, res) => {
    const safeName = path.basename(req.params.restaurantName);
    const tKey = String(req.params.tableNumber);
    db.transaction(() => {
        db.prepare('DELETE FROM tables_state WHERE restaurant = ? AND table_number = ?').run(safeName, tKey);
        db.prepare('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?').run(safeName, tKey);
        db.prepare('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?').run(safeName, tKey);
        db.prepare('DELETE FROM push_subs WHERE restaurant = ? AND table_number = ?').run(safeName, tKey);
    })();
    broadcastState();
    res.json({ success: true });
});

app.get('/api/menu/:restaurantName', (req, res) => {
    const safeName = path.basename(req.params.restaurantName);
    const menuFilePath = path.join(restaurantsDir, `${safeName}.json`);
    if (fs.existsSync(menuFilePath)) {
        res.json(JSON.parse(fs.readFileSync(menuFilePath, 'utf8')));
    } else res.json([]);
});

app.post('/api/menu/:restaurantName', (req, res) => {
    const safeName = path.basename(req.params.restaurantName);
    const menuFilePath = path.join(restaurantsDir, `${safeName}.json`);
    let menu = fs.existsSync(menuFilePath) ? JSON.parse(fs.readFileSync(menuFilePath, 'utf8')) : [];

    const { category, nameEn, nameAr, price } = req.body;
    let cat = menu.find(c => c.categoryEn === category);
    const newItem = { id: uuidv4(), nameEn, nameAr: nameAr || nameEn, price: parseFloat(price) || 0 };
    if (cat) cat.items.push(newItem);
    else menu.push({ categoryEn: category, categoryAr: category, items: [newItem] });

    fs.writeFileSync(menuFilePath, JSON.stringify(menu, null, 4), 'utf8');
    io.emit('menu_updated', { restaurant: safeName, menu });
    res.json({ success: true, item: newItem });
});

app.put('/api/menu/:restaurantName/:itemId', (req, res) => {
    const safeName = path.basename(req.params.restaurantName);
    const menuFilePath = path.join(restaurantsDir, `${safeName}.json`);
    if (!fs.existsSync(menuFilePath)) return res.status(404).json({ success: false });

    let menu = JSON.parse(fs.readFileSync(menuFilePath, 'utf8'));
    const { category, nameEn, nameAr, price } = req.body;
    menu.forEach(cat => { cat.items = cat.items.filter(i => i.id !== req.params.itemId); });

    let cat = menu.find(c => c.categoryEn === category);
    if (!cat) { cat = { categoryEn: category, categoryAr: category, items: [] }; menu.push(cat); }
    cat.items.push({ id: req.params.itemId, nameEn, nameAr: nameAr || nameEn, price: parseFloat(price) || 0 });

    fs.writeFileSync(menuFilePath, JSON.stringify(menu, null, 4), 'utf8');
    io.emit('menu_updated', { restaurant: safeName, menu });
    res.json({ success: true });
});

app.delete('/api/menu/:restaurantName/:itemId', (req, res) => {
    const safeName = path.basename(req.params.restaurantName);
    const menuFilePath = path.join(restaurantsDir, `${safeName}.json`);
    if (!fs.existsSync(menuFilePath)) return res.status(404).json({ success: false });

    let menu = JSON.parse(fs.readFileSync(menuFilePath, 'utf8'));
    menu.forEach(cat => { cat.items = cat.items.filter(i => i.id !== req.params.itemId); });
    fs.writeFileSync(menuFilePath, JSON.stringify(menu, null, 4), 'utf8');
    io.emit('menu_updated', { restaurant: safeName, menu });
    res.json({ success: true });
});

app.get('/api/revenue', (req, res) => {
    const history = db.prepare('SELECT * FROM paid_history').all();
    const report = {};
    history.forEach(ord => {
        const resto = ord.restaurant || 'Default';
        const date = ord.date || new Date().toISOString().split('T')[0];
        const tbl = String(ord.table_number);
        if (!report[resto]) report[resto] = {};
        if (!report[resto][date]) report[resto][date] = {};
        report[resto][date][tbl] = (report[resto][date][tbl] || 0) + (ord.total || 0);
    });
    res.json(report);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));