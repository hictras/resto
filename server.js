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
        idle_minutes INTEGER DEFAULT 360,
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
        start_time INTEGER, last_activity INTEGER, lifetime_minutes INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS push_subs (
        endpoint TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, sub TEXT
    );
    CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        restaurant TEXT NOT NULL,
        table_number TEXT NOT NULL,
        message TEXT NOT NULL,
        icon TEXT DEFAULT '💬',
        created_at INTEGER NOT NULL,
        delivered INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_orders_resto_table ON orders(restaurant, table_number);
    CREATE INDEX IF NOT EXISTS idx_paid_resto_date ON paid_history(restaurant, date);
    CREATE INDEX IF NOT EXISTS idx_sessions_resto_table ON sessions(restaurant, table_number);
    CREATE INDEX IF NOT EXISTS idx_push_resto_table ON push_subs(restaurant, table_number);
    CREATE INDEX IF NOT EXISTS idx_messages_resto_table ON messages(restaurant, table_number, created_at);
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
if (!columnExists('sessions', 'last_activity')) {
    db.exec('ALTER TABLE sessions ADD COLUMN last_activity INTEGER');
    db.prepare('UPDATE sessions SET last_activity = start_time WHERE last_activity IS NULL').run();
}
if (!columnExists('tables_state', 'idle_minutes')) {
    db.exec('ALTER TABLE tables_state ADD COLUMN idle_minutes INTEGER DEFAULT 360');
    db.prepare('UPDATE tables_state SET idle_minutes = 360 WHERE idle_minutes IS NULL').run();
}

// ---------- Push subscription helpers ----------
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

let cachedState = null;
let cacheDirty = true;
function invalidateCache() { cacheDirty = true; }

function buildAppState() {
    // Preload session starts keyed by "restaurant_table"
    const sessionStarts = {};
    db.prepare('SELECT restaurant, table_number, start_time FROM sessions').all().forEach(s => {
        sessionStarts[`${s.restaurant}_${s.table_number}`] = s.start_time;
    });

    // Each table carries its own session + idle info
    const tables = {};
    db.prepare('SELECT * FROM tables_state').all().forEach(r => {
        if (!tables[r.restaurant]) tables[r.restaurant] = {};
        const key = `${r.restaurant}_${r.table_number}`;
        const start = sessionStarts[key];
        tables[r.restaurant][r.table_number] = {
            isOpen: !!r.is_open,
            canOrder: !!r.can_order,
            status: r.status,
            idleMinutes: (r.idle_minutes == null) ? 360 : r.idle_minutes,
            sessionStart: start || null,
            hasSession: !!start
        };
    });

    // Live socket count (used for the "N active" pill)
    const activeSessions = {};
    db.prepare('SELECT restaurant, table_number FROM sessions').all().forEach(s => {
        const sessionKey = `${s.restaurant}_${s.table_number}`;
        const socketCount = (tableSockets[sessionKey] && tableSockets[sessionKey].size) || 0;
        if (socketCount > 0) {
            if (!activeSessions[s.restaurant]) activeSessions[s.restaurant] = {};
            activeSessions[s.restaurant][s.table_number] = socketCount;
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
function broadcastState() { invalidateCache(); io.emit('state_update', getAppState()); }

// Session valid if:
//   idleMinutes === 0  → no expiry (lives until bill is paid)
//   otherwise          → now - start_time <= idleMinutes (total session lifetime)
function isSessionValid(session, resto, table) {
    if (!session) return false;
    let idleMinutes = 360;
    try {
        const row = db.prepare('SELECT idle_minutes FROM tables_state WHERE restaurant = ? AND table_number = ?').get(resto, String(table));
        if (row && row.idle_minutes != null) idleMinutes = row.idle_minutes;
    } catch (e) {}
    if (idleMinutes === 0) return true;
    const start = session.start_time || 0;
    return (Date.now() - start) <= (idleMinutes * 60 * 1000);
}

// No-op — activity no longer resets the session clock (Option B).
function touchSession(_token) { /* intentionally empty */ }

// ---------- Message helpers ----------
function getMessagesForTable(resto, table, limit = 100) {
    return db.prepare(`SELECT id, message, icon, created_at, delivered
                       FROM messages WHERE restaurant = ? AND table_number = ?
                       ORDER BY created_at ASC LIMIT ?`)
        .all(resto, String(table), limit)
        .map(m => ({ id: m.id, message: m.message, icon: m.icon, createdAt: m.created_at, delivered: !!m.delivered }));
}

function undeliveredMessagesForTable(resto, table) {
    return db.prepare(`SELECT id, message, icon, created_at FROM messages
                       WHERE restaurant = ? AND table_number = ? AND delivered = 0
                       ORDER BY created_at ASC`)
        .all(resto, String(table))
        .map(m => ({ id: m.id, message: m.message, icon: m.icon, createdAt: m.created_at }));
}

function seedWelcomeMessage(resto, table) {
    const id = uuidv4();
    const text = "Welcome! 👋 We're delighted to have you here. Browse our menu and ring the bell when you're ready to order.";
    db.prepare(`INSERT INTO messages (id, restaurant, table_number, message, icon, created_at, delivered)
                VALUES (?, ?, ?, ?, '👋', ?, 0)`)
        .run(id, resto, String(table), text, Date.now());
    return id;
}

function endTableSession(resto, table) {
    const tKey = String(table);
    db.transaction(() => {
        db.prepare('UPDATE tables_state SET is_open = 0, can_order = 0 WHERE restaurant = ? AND table_number = ?')
            .run(resto, tKey);
        db.prepare('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        db.prepare('DELETE FROM messages WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        db.prepare('DELETE FROM push_subs WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        db.prepare('DELETE FROM orders WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
        db.prepare('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?').run(resto, tKey);
    })();
}

function kickTableSockets(resto, table) {
    const tKey = String(table);
    const room = `table_${resto}_${tKey}`;
    const socketsInRoom = io.sockets.adapter.rooms.get(room);
    if (socketsInRoom) {
        for (const sid of Array.from(socketsInRoom)) {
            const s = io.sockets.sockets.get(sid);
            if (s) {
                s.leave(room);
                s.tableSessionKey = null;
                s.tableToken = null;
            }
        }
    }
    if (tableSockets[`${resto}_${tKey}`]) delete tableSockets[`${resto}_${tKey}`];
}

// ---------- Expiry sweep (every 30 s) ----------
setInterval(() => {
    const now = Date.now();
    const rows = db.prepare(`
        SELECT s.restaurant, s.table_number, s.start_time,
               COALESCE(t.idle_minutes, 360) AS idle_minutes
        FROM sessions s
        LEFT JOIN tables_state t
          ON t.restaurant = s.restaurant AND t.table_number = s.table_number
    `).all();

    const expired = [];
    rows.forEach(s => {
        if (s.idle_minutes === 0) return;
        const start = s.start_time || 0;
        if (now - start > s.idle_minutes * 60 * 1000) {
            expired.push({ restaurant: s.restaurant, table_number: s.table_number });
        }
    });

    if (expired.length === 0) return;

    expired.forEach(s => {
        endTableSession(s.restaurant, s.table_number);
        io.to(`table_${s.restaurant}_${s.table_number}`).emit('session_ended', { reason: 'expired' });
        kickTableSockets(s.restaurant, s.table_number);
    });
    broadcastState();
}, 30 * 1000);

// ---------- API ----------
app.get('/api/verify-token', (req, res) => {
    const { token } = req.query;
    if (!token) return res.status(400).json({ success: false, message: 'Missing token' });
    const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
    if (!isSessionValid(session, session && session.restaurant, session && session.table_number)) {
        return res.status(401).json({ success: false, message: 'Invalid or expired token' });
    }
    res.json({
        success: true,
        restaurant: session.restaurant,
        tableNumber: session.table_number,
        lifetimeMinutes: null,
        expiresAt: null,
        sessionStart: session.start_time
    });
});

// ---------- Create session (with 30s grace window for multi-device scans) ----------
app.post('/api/create-session', (req, res) => {
    const { restaurant, tableNumber } = req.body;
    if (!restaurant || !tableNumber) {
        return res.status(400).json({ success: false, message: 'Restaurant and table number required' });
    }
    try {
        const resto = path.basename(String(restaurant));
        const tKey = String(tableNumber);
        const now = Date.now();

        const existing = db.prepare('SELECT * FROM sessions WHERE restaurant = ? AND table_number = ?').get(resto, tKey);

        if (existing) {
            const age = now - (existing.start_time || 0);
            const withinGracePeriod = age < 30 * 1000;           // same-party multi-scan
            const stillValid = isSessionValid(existing, resto, tKey);

            if (withinGracePeriod && stillValid) {
                return res.json({
                    success: true,
                    token: existing.token,
                    expiresAt: null,
                    lifetimeMinutes: null,
                    reused: true
                });
            }
        }

        // New party — wipe any prior session and start fresh
        if (existing) endTableSession(resto, tKey);

        const token = 'tok_' + uuidv4().replace(/-/g, '') + now.toString(36);
        db.transaction(() => {
            db.prepare(`INSERT INTO sessions (token, restaurant, table_number, start_time, last_activity, lifetime_minutes)
                        VALUES (?, ?, ?, ?, ?, 0)`)
                .run(token, resto, tKey, now, now);
            seedWelcomeMessage(resto, tKey);
        })();

        broadcastState();
        res.json({
            success: true,
            token,
            expiresAt: null,
            lifetimeMinutes: null,
            reused: false
        });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: 'Internal server error' });
    }
});

app.get('/api/messages/:restaurantName/:tableNumber', (req, res) => {
    const resto = path.basename(req.params.restaurantName);
    const table = String(req.params.tableNumber);
    res.json(getMessagesForTable(resto, table));
});

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
    socket.emit('state_update', getAppState());

    socket.on('register_session', ({ token }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
        if (!isSessionValid(session, session && session.restaurant, session && session.table_number)) {
            return cb({ success: false, message: 'Invalid token' });
        }

        const resto = session.restaurant;
        const tKey = session.table_number;
        const sessionKey = `${resto}_${tKey}`;

        if (!tableSockets[sessionKey]) tableSockets[sessionKey] = new Set();
        tableSockets[sessionKey].add(socket.id);
        socket.tableSessionKey = sessionKey;
        socket.tableToken = token;
        socket.join(`table_${resto}_${tKey}`);

        const undelivered = undeliveredMessagesForTable(resto, tKey);
        if (undelivered.length > 0) {
            socket.emit('admin_messages_bulk', { messages: undelivered });
            const ids = undelivered.map(m => m.id);
            const placeholders = ids.map(() => '?').join(',');
            db.prepare(`UPDATE messages SET delivered = 1 WHERE id IN (${placeholders})`).run(...ids);
        }

        broadcastState();
        cb({ success: true, restaurant: resto, tableNumber: tKey });
    });

    socket.on('update_cart', ({ token, cart }) => {
        const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
        if (!isSessionValid(session, session && session.restaurant, session && session.table_number)) return;
        socket.to(`table_${session.restaurant}_${session.table_number}`).emit('cart_synced', { cart });
    });

    socket.on('disconnect', () => {
        const sessionKey = socket.tableSessionKey;
        if (sessionKey && tableSockets[sessionKey]) {
            tableSockets[sessionKey].delete(socket.id);
            if (tableSockets[sessionKey].size === 0) {
                delete tableSockets[sessionKey];
            }
            broadcastState();
        }
    });

    socket.on('set_table_idle', ({ restaurant, tableNum, idleMinutes }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        const resto = path.basename(String(restaurant || 'Default'));
        const tKey = String(tableNum);
        const minutes = Math.max(0, Math.min(1440 * 7, parseInt(idleMinutes) || 0)); // 0..7 days

        const row = db.prepare('SELECT 1 FROM tables_state WHERE restaurant = ? AND table_number = ?').get(resto, tKey);
        if (!row) return cb({ success: false, message: 'Table not found' });

        db.transaction(() => {
            db.prepare('UPDATE tables_state SET idle_minutes = ? WHERE restaurant = ? AND table_number = ?')
                .run(minutes, resto, tKey);

            // Restart the session clock from now if a session is alive
            const sess = db.prepare('SELECT token FROM sessions WHERE restaurant = ? AND table_number = ?').get(resto, tKey);
            if (sess) {
                const now = Date.now();
                db.prepare('UPDATE sessions SET start_time = ?, last_activity = ? WHERE token = ?')
                    .run(now, now, sess.token);
            }
        })();

        broadcastState();
        cb({ success: true, idleMinutes: minutes });
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
        if (!isSessionValid(session, session && session.restaurant, session && session.table_number)) return;
        const resto = session.restaurant;
        const tKey = session.table_number;
        const now = Date.now();
        db.prepare(`INSERT INTO table_requests (restaurant, table_number, requested) VALUES (?, ?, 1)
                    ON CONFLICT(restaurant, table_number) DO UPDATE SET requested = 1`)
            .run(resto, tKey);
        io.emit('bell_ring', { restaurant: resto, tableNumber: tKey, at: now });
        broadcastState();
    });

    socket.on('place_order', ({ items, total, token }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const session = db.prepare('SELECT * FROM sessions WHERE token = ?').get(token);
            if (!isSessionValid(session, session && session.restaurant, session && session.table_number)) {
                return cb({ success: false, message: 'Session expired or unauthorized.' });
            }

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

        if (billStatus !== 'Paid') {
            db.prepare('UPDATE orders SET bill_status = ? WHERE order_id = ?').run(billStatus, orderId);
            broadcastState();
            return;
        }

        const resto = order.restaurant;
        const tKey = order.table_number;

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
        ).get(resto, tKey).c;

        if (remainingUnpaid === 0) {
            endTableSession(resto, tKey);
            io.to(`table_${resto}_${tKey}`).emit('session_ended', { reason: 'paid' });
            kickTableSockets(resto, tKey);
        }

        broadcastState();
    });

    socket.on('clear_bill', ({ orderId }) => {
        db.prepare('DELETE FROM orders WHERE order_id = ?').run(orderId);
        broadcastState();
    });

    socket.on('reset_table_session', ({ restaurant, tableNum }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        const resto = path.basename(String(restaurant || 'Default'));
        const tKey = String(tableNum);

        const activeOrders = db.prepare(
            `SELECT COUNT(*) AS c FROM orders WHERE restaurant = ? AND table_number = ?`
        ).get(resto, tKey).c;

        if (activeOrders > 0) {
            return cb({ success: false, message: `Table has ${activeOrders} active order(s). Mark them Paid or clear them first.` });
        }

        endTableSession(resto, tKey);
        io.to(`table_${resto}_${tKey}`).emit('session_ended', { reason: 'reset' });
        kickTableSockets(resto, tKey);
        broadcastState();
        cb({ success: true });
    });

    socket.on('admin_message', ({ restaurant, tableNum, message, icon }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        const resto = path.basename(String(restaurant || 'Default'));
        const tKey = String(tableNum);
        const text = String(message || '').trim().slice(0, 500);
        if (!text) return cb({ success: false, message: 'Empty message' });

        const id = uuidv4();
        const createdAt = Date.now();
        const safeIcon = String(icon || '💬').slice(0, 8);

        db.prepare(`INSERT INTO messages (id, restaurant, table_number, message, icon, created_at, delivered)
                    VALUES (?, ?, ?, ?, ?, ?, 0)`)
            .run(id, resto, tKey, text, safeIcon, createdAt);

        const room = `table_${resto}_${tKey}`;
        const roomSockets = io.sockets.adapter.rooms.get(room);
        const isLive = !!(roomSockets && roomSockets.size > 0);
        if (isLive) {
            io.to(room).emit('admin_message', { id, message: text, icon: safeIcon, createdAt });
            db.prepare('UPDATE messages SET delivered = 1 WHERE id = ?').run(id);
        }

        const subs = getPushSubs(resto, tKey);
        const payload = JSON.stringify({
            title: `${safeIcon} Message from staff`,
            body: text,
            url: '/menu.html'
        });
        subs.forEach(sub => {
            webpush.sendNotification(sub, payload).catch(err => {
                if (err.statusCode === 404 || err.statusCode === 410) removePushSub(sub.endpoint);
            });
        });

        cb({ success: true, id, delivered: isLive });
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

// ---------- QR links (no session creation, no broadcast) ----------
app.get('/api/qr-links', (req, res) => {
    try {
        const targetResto = req.query.restaurant;
        let files = fs.readdirSync(restaurantsDir).filter(f => f.endsWith('.json'));
        let restaurants = files.map(f => path.basename(f, '.json'));
        if (targetResto) restaurants = restaurants.filter(r => r === targetResto);

        const allLinks = [];
        restaurants.forEach(resto => {
            const restoTables = db.prepare('SELECT table_number FROM tables_state WHERE restaurant = ?').all(resto);
            restoTables.forEach(t => {
                const tableNumStr = String(t.table_number);
                const encodedTbl = Buffer.from(tableNumStr).toString('base64').split('').reverse().join('');
                const encodedResto = Buffer.from(resto).toString('base64').split('').reverse().join('');

                allLinks.push({
                    restaurant: resto,
                    tableNumber: t.table_number,
                    encodedUrl: `/customer.html?tbl=${encodeURIComponent(encodedTbl)}&rst=${encodeURIComponent(encodedResto)}`
                });
            });
        });
        // Nothing changed in the DB → no broadcastState()
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

    db.prepare(`INSERT INTO tables_state (restaurant, table_number, is_open, can_order, status, idle_minutes)
                VALUES (?, ?, 0, 0, 'Valid', 360)
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
        db.prepare('DELETE FROM messages WHERE restaurant = ? AND table_number = ?').run(safeName, tKey);
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