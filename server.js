require('dotenv').config();

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const webpush = require('web-push');
const multer = require('multer');
const { createClient } = require('@libsql/client');
const { S3Client, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// ---------- Admin auth ----------
const ADMIN_PASSWORD = 'admin123';   // ← change this to your password
const adminSessions = new Map();
const ADMIN_SESSION_MS = 12 * 60 * 60 * 1000;

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true, limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Data dirs ----------
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
const restaurantsDir = path.join(__dirname, 'restaurants');
if (!fs.existsSync(restaurantsDir)) fs.mkdirSync(restaurantsDir, { recursive: true });
const dishPhotosDir = path.join(__dirname, 'public', 'dish-photos');
if (!fs.existsSync(dishPhotosDir)) fs.mkdirSync(dishPhotosDir, { recursive: true });

// ---------- Turso client ----------
const db = createClient({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
});

async function dbRun(sql, args = []) { return db.execute({ sql, args }); }
async function dbGet(sql, args = []) {
    const res = await db.execute({ sql, args });
    return res.rows[0] || null;
}
async function dbAll(sql, args = []) {
    const res = await db.execute({ sql, args });
    return res.rows;
}

// ---------- Cloudflare R2 client ----------
const r2Client = new S3Client({
    region: 'auto',
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    },
});

// ---------- Multer ----------
const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 5 * 1024 * 1024 },
    fileFilter: (req, file, cb) => {
        const ok = /\.(jpe?g|png|webp)$/i.test(file.originalname);
        cb(ok ? null : new Error('Only JPG / PNG / WEBP allowed'), ok);
    }
});

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

// ---------- Schema bootstrap ----------
let schemaReady = false;
async function ensureSchema() {
    if (schemaReady) return;

    const statements = [
        `CREATE TABLE IF NOT EXISTS tables_state (
            restaurant TEXT, table_number TEXT, is_open INTEGER, can_order INTEGER, status TEXT,
            idle_minutes INTEGER DEFAULT 360,
            PRIMARY KEY (restaurant, table_number)
        )`,
        `CREATE TABLE IF NOT EXISTS table_requests (
            restaurant TEXT, table_number TEXT, requested INTEGER,
            PRIMARY KEY (restaurant, table_number)
        )`,
        `CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, items TEXT,
            total REAL, kitchen_status TEXT, bill_status TEXT, date TEXT, timestamp TEXT, created_at INTEGER
        )`,
        `CREATE TABLE IF NOT EXISTS paid_history (
            order_id TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, items TEXT,
            total REAL, kitchen_status TEXT, bill_status TEXT, date TEXT, timestamp TEXT, created_at INTEGER
        )`,
        `CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT,
            start_time INTEGER, last_activity INTEGER, lifetime_minutes INTEGER DEFAULT 0
        )`,
        `CREATE TABLE IF NOT EXISTS push_subs (
            endpoint TEXT PRIMARY KEY, restaurant TEXT, table_number TEXT, sub TEXT
        )`,
        `CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            restaurant TEXT NOT NULL,
            table_number TEXT NOT NULL,
            message TEXT NOT NULL,
            icon TEXT DEFAULT '💬',
            created_at INTEGER NOT NULL,
            delivered INTEGER DEFAULT 0
        )`,
        `CREATE TABLE IF NOT EXISTS menu_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant TEXT NOT NULL,
            category_en TEXT NOT NULL,
            category_ar TEXT,
            UNIQUE(restaurant, category_en)
        )`,
        `CREATE TABLE IF NOT EXISTS menu_items (
            id TEXT NOT NULL,
            restaurant TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            name_en TEXT NOT NULL,
            name_ar TEXT,
            price REAL NOT NULL,
            desc_en TEXT,
            desc_ar TEXT,
            image TEXT,
            PRIMARY KEY (restaurant, id),
            FOREIGN KEY (category_id) REFERENCES menu_categories(id) ON DELETE CASCADE
        )`,
        `CREATE TABLE IF NOT EXISTS settings (
            restaurant TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            PRIMARY KEY (restaurant, key)
        )`,
        'CREATE INDEX IF NOT EXISTS idx_orders_resto_table ON orders(restaurant, table_number)',
        'CREATE INDEX IF NOT EXISTS idx_paid_resto_date ON paid_history(restaurant, date)',
        'CREATE INDEX IF NOT EXISTS idx_sessions_resto_table ON sessions(restaurant, table_number)',
        'CREATE INDEX IF NOT EXISTS idx_messages_resto_table ON messages(restaurant, table_number, created_at)',
        'CREATE INDEX IF NOT EXISTS idx_menu_items_resto ON menu_items(restaurant)',
        'CREATE INDEX IF NOT EXISTS idx_menu_categories_resto ON menu_categories(restaurant)',
    ];

    for (const stmt of statements) {
        await db.execute(stmt);
    }

    // Graceful column additions for existing DBs
    for (const col of ['sort INTEGER DEFAULT 0', 'dietary TEXT']) {
        try { await db.execute(`ALTER TABLE menu_items ADD COLUMN ${col}`); }
        catch (e) { /* duplicate column — ignore */ }
    }
    for (const col of ['location TEXT', 'capacity INTEGER', 'party_size INTEGER']) {
        try { await db.execute(`ALTER TABLE tables_state ADD COLUMN ${col}`); }
        catch (e) { /* duplicate column — ignore */ }
    }

    schemaReady = true;
    console.log('✅ Schema ready');
}

// ---------- Push helpers ----------
async function savePushSub(resto, table, sub) {
    await dbRun(
        'INSERT OR REPLACE INTO push_subs (endpoint, restaurant, table_number, sub) VALUES (?, ?, ?, ?)',
        [sub.endpoint, resto, String(table), JSON.stringify(sub)]
    );
}
async function getPushSubs(resto, table) {
    const rows = await dbAll(
        'SELECT sub FROM push_subs WHERE restaurant = ? AND table_number = ?',
        [resto, String(table)]
    );
    return rows.map(r => JSON.parse(r.sub));
}
async function removePushSub(endpoint) {
    await dbRun('DELETE FROM push_subs WHERE endpoint = ?', [endpoint]);
}

app.post('/api/subscribe-push', async (req, res) => {
    try {
        const { restaurant, tableNumber, subscription } = req.body;
        if (!restaurant || !tableNumber || !subscription || !subscription.endpoint) {
            return res.status(400).json({ success: false, message: 'Missing fields' });
        }
        await savePushSub(path.basename(String(restaurant)), String(tableNumber), subscription);
        res.status(201).json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false });
    }
});

// ---------- Socket state ----------
const tableSockets = {};
let cachedState = null;
let cacheDirty = true;
function invalidateCache() { cacheDirty = true; }

async function buildAppState() {
    const sessionStarts = {};
    (await dbAll('SELECT restaurant, table_number, start_time FROM sessions')).forEach(s => {
        sessionStarts[`${s.restaurant}_${s.table_number}`] = s.start_time;
    });

    const tables = {};
    (await dbAll('SELECT * FROM tables_state')).forEach(r => {
        if (!tables[r.restaurant]) tables[r.restaurant] = {};
        const key = `${r.restaurant}_${r.table_number}`;
        const start = sessionStarts[key];
        tables[r.restaurant][r.table_number] = {
            isOpen: !!r.is_open,
            canOrder: !!r.can_order,
            status: r.status,
            idleMinutes: (r.idle_minutes == null) ? 360 : r.idle_minutes,
            location: r.location || null,
            capacity: (r.capacity == null) ? null : Number(r.capacity),
            partySize: (r.party_size == null) ? null : Number(r.party_size),
            sessionStart: start || null,
            hasSession: !!start
        };
    });

    const activeSessions = {};
    (await dbAll('SELECT restaurant, table_number FROM sessions')).forEach(s => {
        const sessionKey = `${s.restaurant}_${s.table_number}`;
        const socketCount = (tableSockets[sessionKey] && tableSockets[sessionKey].size) || 0;
        if (socketCount > 0) {
            if (!activeSessions[s.restaurant]) activeSessions[s.restaurant] = {};
            activeSessions[s.restaurant][s.table_number] = socketCount;
        }
    });

    const tableRequests = {};
    (await dbAll('SELECT * FROM table_requests WHERE requested = 1')).forEach(r => {
        if (!tableRequests[r.restaurant]) tableRequests[r.restaurant] = {};
        tableRequests[r.restaurant][r.table_number] = true;
    });

    const orders = (await dbAll('SELECT * FROM orders')).map(o => ({
        orderId: o.order_id, restaurant: o.restaurant, tableNumber: o.table_number,
        items: JSON.parse(o.items), total: o.total, kitchenStatus: o.kitchen_status,
        billStatus: o.bill_status, date: o.date, timestamp: o.timestamp, createdAt: o.created_at
    }));

    return { tables, tableRequests, orders, activeSessions };
}

async function getAppState() {
    if (cacheDirty || !cachedState) {
        cachedState = await buildAppState();
        cacheDirty = false;
    }
    return cachedState;
}
async function broadcastState() {
    invalidateCache();
    const state = await getAppState();
    io.emit('state_update', state);
}

async function isSessionValid(session, resto, table) {
    if (!session) return false;
    let idleMinutes = 360;
    try {
        const row = await dbGet(
            'SELECT idle_minutes FROM tables_state WHERE restaurant = ? AND table_number = ?',
            [resto, String(table)]
        );
        if (row && row.idle_minutes != null) idleMinutes = row.idle_minutes;
    } catch (e) {}
    if (idleMinutes === 0) return true;
    const start = session.start_time || 0;
    return (Date.now() - start) <= (idleMinutes * 60 * 1000);
}

// ---------- Message helpers ----------
async function getMessagesForTable(resto, table, limit = 100) {
    const rows = await dbAll(
        `SELECT id, message, icon, created_at, delivered
         FROM messages WHERE restaurant = ? AND table_number = ?
         ORDER BY created_at ASC LIMIT ?`,
        [resto, String(table), limit]
    );
    return rows.map(m => ({
        id: m.id, message: m.message, icon: m.icon,
        createdAt: m.created_at, delivered: !!m.delivered
    }));
}

async function undeliveredMessagesForTable(resto, table) {
    const rows = await dbAll(
        `SELECT id, message, icon, created_at FROM messages
         WHERE restaurant = ? AND table_number = ? AND delivered = 0
         ORDER BY created_at ASC`,
        [resto, String(table)]
    );
    return rows.map(m => ({ id: m.id, message: m.message, icon: m.icon, createdAt: m.created_at }));
}

async function seedWelcomeMessage(resto, table) {
    const id = uuidv4();
    const text = "Welcome! 👋 We're delighted to have you here. Browse our menu and ring the bell when you're ready to order.";
    await dbRun(
        `INSERT INTO messages (id, restaurant, table_number, message, icon, created_at, delivered)
         VALUES (?, ?, ?, ?, '👋', ?, 0)`,
        [id, resto, String(table), text, Date.now()]
    );
    return id;
}

async function endTableSession(resto, table) {
    const tKey = String(table);
    await dbRun('UPDATE tables_state SET is_open = 0, can_order = 0, party_size = NULL WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
    await dbRun('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
    await dbRun('DELETE FROM messages WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
    await dbRun('DELETE FROM push_subs WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
    await dbRun('DELETE FROM orders WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
    await dbRun('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
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

// ---------- Expiry sweep ----------
setInterval(async () => {
    try {
        const now = Date.now();
        const rows = await dbAll(`
            SELECT s.restaurant, s.table_number, s.start_time,
                   COALESCE(t.idle_minutes, 360) AS idle_minutes
            FROM sessions s
            LEFT JOIN tables_state t
              ON t.restaurant = s.restaurant AND t.table_number = s.table_number
        `);

        const expired = [];
        rows.forEach(s => {
            if (s.idle_minutes === 0) return;
            const start = s.start_time || 0;
            if (now - start > s.idle_minutes * 60 * 1000) {
                expired.push({ restaurant: s.restaurant, table_number: s.table_number });
            }
        });

        if (expired.length === 0) return;

        for (const s of expired) {
            await endTableSession(s.restaurant, s.table_number);
            io.to(`table_${s.restaurant}_${s.table_number}`).emit('session_ended', { reason: 'expired' });
            kickTableSockets(s.restaurant, s.table_number);
        }
        await broadcastState();
    } catch (e) {
        console.error('Expiry sweep error:', e);
    }
}, 30 * 1000);

// ---------- API ----------
app.get('/api/verify-token', async (req, res) => {
    try {
        const { token } = req.query;
        if (!token) return res.status(400).json({ success: false, message: 'Missing token' });
        const session = await dbGet('SELECT * FROM sessions WHERE token = ?', [token]);
        if (!(await isSessionValid(session, session && session.restaurant, session && session.table_number))) {
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
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false });
    }
});

app.post('/api/create-session', async (req, res) => {
    try {
        const { restaurant, tableNumber } = req.body;
        if (!restaurant || !tableNumber) {
            return res.status(400).json({ success: false, message: 'Restaurant and table number required' });
        }
        const resto = path.basename(String(restaurant));
        const tKey = String(tableNumber);
        const now = Date.now();

        const existing = await dbGet('SELECT * FROM sessions WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
        if (existing && (await isSessionValid(existing, resto, tKey))) {
            return res.json({ success: true, token: existing.token, expiresAt: null, lifetimeMinutes: null, reused: true });
        }

        if (existing) await endTableSession(resto, tKey);

        const token = 'tok_' + uuidv4().replace(/-/g, '') + now.toString(36);
        await dbRun(
            `INSERT INTO sessions (token, restaurant, table_number, start_time, last_activity, lifetime_minutes)
             VALUES (?, ?, ?, ?, ?, 0)`,
            [token, resto, tKey, now, now]
        );
        await seedWelcomeMessage(resto, tKey);

        await broadcastState();
        res.json({ success: true, token, expiresAt: null, lifetimeMinutes: null, reused: false });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: 'Internal server error' });
    }
});

app.get('/api/messages/:restaurantName/:tableNumber', async (req, res) => {
    try {
        const resto = path.basename(req.params.restaurantName);
        const table = String(req.params.tableNumber);
        res.json(await getMessagesForTable(resto, table));
    } catch (e) {
        console.error(e);
        res.status(500).json([]);
    }
});

// ---------- Socket.IO ----------
io.on('connection', (socket) => {
    (async () => {
        try { socket.emit('state_update', await getAppState()); }
        catch (e) { console.error(e); }
    })();

    socket.on('register_session', async ({ token }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const session = await dbGet('SELECT * FROM sessions WHERE token = ?', [token]);
            if (!(await isSessionValid(session, session && session.restaurant, session && session.table_number))) {
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

            const undelivered = await undeliveredMessagesForTable(resto, tKey);
            if (undelivered.length > 0) {
                socket.emit('admin_messages_bulk', { messages: undelivered });
                const ids = undelivered.map(m => m.id);
                const placeholders = ids.map(() => '?').join(',');
                await dbRun(`UPDATE messages SET delivered = 1 WHERE id IN (${placeholders})`, ids);
            }

            await broadcastState();
            cb({ success: true, restaurant: resto, tableNumber: tKey });
        } catch (e) {
            console.error(e);
            cb({ success: false, message: 'Server error' });
        }
    });

    socket.on('update_cart', async ({ token, cart }) => {
        try {
            const session = await dbGet('SELECT * FROM sessions WHERE token = ?', [token]);
            if (!(await isSessionValid(session, session && session.restaurant, session && session.table_number))) return;
            socket.to(`table_${session.restaurant}_${session.table_number}`).emit('cart_synced', { cart });
        } catch (e) {}
    });

    socket.on('disconnect', async () => {
        const sessionKey = socket.tableSessionKey;
        if (sessionKey && tableSockets[sessionKey]) {
            tableSockets[sessionKey].delete(socket.id);
            if (tableSockets[sessionKey].size === 0) delete tableSockets[sessionKey];
            await broadcastState();
        }
    });

    socket.on('set_table_idle', async ({ restaurant, tableNum, idleMinutes }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const resto = path.basename(String(restaurant || 'Default'));
            const tKey = String(tableNum);
            const minutes = Math.max(0, Math.min(1440 * 7, parseInt(idleMinutes) || 0));

            const row = await dbGet('SELECT 1 AS ok FROM tables_state WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            if (!row) return cb({ success: false, message: 'Table not found' });

            await dbRun('UPDATE tables_state SET idle_minutes = ? WHERE restaurant = ? AND table_number = ?', [minutes, resto, tKey]);

            const sess = await dbGet('SELECT token FROM sessions WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            if (sess) {
                const now = Date.now();
                await dbRun('UPDATE sessions SET start_time = ?, last_activity = ? WHERE token = ?', [now, now, sess.token]);
            }

            await broadcastState();
            cb({ success: true, idleMinutes: minutes });
        } catch (e) {
            console.error(e);
            cb({ success: false, message: 'Server error' });
        }
    });

    socket.on('toggle_table', async ({ restaurant, tableNum }) => {
        try {
            const resto = restaurant || 'Default';
            const tKey = String(tableNum);
            const current = await dbGet('SELECT is_open FROM tables_state WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            if (!current) return;
            const newOpen = current.is_open ? 0 : 1;
            if (newOpen === 0) {
                await dbRun('UPDATE tables_state SET is_open = 0, can_order = 0 WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
                await dbRun('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            } else {
                await dbRun('UPDATE tables_state SET is_open = 1 WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            }
            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('toggle_order_permission', async ({ restaurant, tableNum }) => {
        try {
            const resto = restaurant || 'Default';
            const tKey = String(tableNum);
            const current = await dbGet('SELECT can_order FROM tables_state WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            if (!current) return;
            const newCanOrder = current.can_order ? 0 : 1;
            await dbRun('UPDATE tables_state SET can_order = ? WHERE restaurant = ? AND table_number = ?', [newCanOrder, resto, tKey]);
            if (newCanOrder) {
                await dbRun('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            }
            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('table_request', async ({ token }) => {
        try {
            const session = await dbGet('SELECT * FROM sessions WHERE token = ?', [token]);
            if (!(await isSessionValid(session, session && session.restaurant, session && session.table_number))) return;
            const resto = session.restaurant;
            const tKey = session.table_number;
            const now = Date.now();
            await dbRun(
                `INSERT INTO table_requests (restaurant, table_number, requested) VALUES (?, ?, 1)
                 ON CONFLICT(restaurant, table_number) DO UPDATE SET requested = 1`,
                [resto, tKey]
            );
            io.emit('bell_ring', { restaurant: resto, tableNumber: tKey, at: now });
            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('place_order', async ({ items, total, token }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const session = await dbGet('SELECT * FROM sessions WHERE token = ?', [token]);
            if (!(await isSessionValid(session, session && session.restaurant, session && session.table_number))) {
                return cb({ success: false, message: 'Session expired or unauthorized.' });
            }

            const resto = session.restaurant;
            const tKey = session.table_number;
            const tableState = await dbGet('SELECT * FROM tables_state WHERE restaurant = ? AND table_number = ?', [resto, tKey]);
            if (!tableState || !tableState.is_open || !tableState.can_order) {
                return cb({ success: false, message: 'Order submission is locked by the manager.' });
            }

            const orderId = uuidv4();
            const date = new Date().toISOString().split('T')[0];
            const timestamp = new Date().toLocaleTimeString();
            const createdAt = Date.now();
            const formattedItems = JSON.stringify(items.map(i => ({ ...i, checks: [false, false] })));

            await dbRun(
                `INSERT INTO orders (order_id, restaurant, table_number, items, total, kitchen_status, bill_status, date, timestamp, created_at)
                 VALUES (?, ?, ?, ?, ?, 'Received', 'Unpaid', ?, ?, ?)`,
                [orderId, resto, tKey, formattedItems, total, date, timestamp, createdAt]
            );
            await dbRun('UPDATE tables_state SET can_order = 0 WHERE restaurant = ? AND table_number = ?', [resto, tKey]);

            io.to(`table_${resto}_${tKey}`).emit('order_placed_success');
            await broadcastState();
            cb({ success: true, orderId });
        } catch (e) {
            console.error('place_order failed:', e);
            cb({ success: false, message: 'Server error.' });
        }
    });

    socket.on('update_kitchen_status', async ({ orderId, kitchenStatus }) => {
        try {
            await dbRun('UPDATE orders SET kitchen_status = ? WHERE order_id = ?', [kitchenStatus, orderId]);
            const order = await dbGet('SELECT * FROM orders WHERE order_id = ?', [orderId]);
            if (order) {
                const subs = await getPushSubs(order.restaurant, order.table_number);
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
            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('toggle_item_check', async ({ orderId, itemIndex, checkIndex }) => {
        try {
            const orderRow = await dbGet('SELECT items FROM orders WHERE order_id = ?', [orderId]);
            if (!orderRow) return;
            const items = JSON.parse(orderRow.items);
            if (!items[itemIndex]) return;
            if (!items[itemIndex].checks) items[itemIndex].checks = [false, false];
            items[itemIndex].checks[checkIndex] = !items[itemIndex].checks[checkIndex];

            const anyCook = items.some(i => i.checks && i.checks[0] && !i.checks[1]);
            const allReady = items.every(i => i.checks && i.checks[1]);
            const statusUpdate = allReady ? 'Ready' : (anyCook ? 'Cooking' : 'Received');

            await dbRun('UPDATE orders SET items = ?, kitchen_status = ? WHERE order_id = ?', [JSON.stringify(items), statusUpdate, orderId]);
            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('update_bill_status', async ({ orderId, billStatus }) => {
        try {
            const order = await dbGet('SELECT * FROM orders WHERE order_id = ?', [orderId]);
            if (!order) return;

            if (billStatus !== 'Paid') {
                await dbRun('UPDATE orders SET bill_status = ? WHERE order_id = ?', [billStatus, orderId]);
                await broadcastState();
                return;
            }

            const resto = order.restaurant;
            const tKey = order.table_number;

            await dbRun(
                `INSERT OR REPLACE INTO paid_history
                 (order_id, restaurant, table_number, items, total, kitchen_status, bill_status, date, timestamp, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [order.order_id, order.restaurant, order.table_number, order.items,
                 order.total, order.kitchen_status, 'Paid', order.date, order.timestamp,
                 order.created_at || Date.now()]
            );
            await dbRun('DELETE FROM orders WHERE order_id = ?', [orderId]);

            const remainingUnpaid = await dbGet(
                `SELECT COUNT(*) AS c FROM orders WHERE restaurant = ? AND table_number = ? AND bill_status != 'Paid'`,
                [resto, tKey]
            );

            if (!remainingUnpaid || remainingUnpaid.c === 0) {
                await endTableSession(resto, tKey);
                io.to(`table_${resto}_${tKey}`).emit('session_ended', { reason: 'paid' });
                kickTableSockets(resto, tKey);
            }

            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('clear_bill', async ({ orderId }) => {
        try {
            await dbRun('DELETE FROM orders WHERE order_id = ?', [orderId]);
            await broadcastState();
        } catch (e) { console.error(e); }
    });

    socket.on('reset_table_session', async ({ restaurant, tableNum }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const resto = path.basename(String(restaurant || 'Default'));
            const tKey = String(tableNum);

            const activeOrders = await dbGet(
                `SELECT COUNT(*) AS c FROM orders WHERE restaurant = ? AND table_number = ?`,
                [resto, tKey]
            );

            if (activeOrders && activeOrders.c > 0) {
                return cb({ success: false, message: `Table has ${activeOrders.c} active order(s). Mark them Paid or clear them first.` });
            }

            await endTableSession(resto, tKey);
            io.to(`table_${resto}_${tKey}`).emit('session_ended', { reason: 'reset' });
            kickTableSockets(resto, tKey);
            await broadcastState();
            cb({ success: true });
        } catch (e) {
            console.error(e);
            cb({ success: false, message: 'Server error' });
        }
    });

    socket.on('admin_message', async ({ restaurant, tableNum, message, icon }, callback) => {
        const cb = typeof callback === 'function' ? callback : () => {};
        try {
            const resto = path.basename(String(restaurant || 'Default'));
            const tKey = String(tableNum);
            const text = String(message || '').trim().slice(0, 500);
            if (!text) return cb({ success: false, message: 'Empty message' });

            const id = uuidv4();
            const createdAt = Date.now();
            const safeIcon = String(icon || '💬').slice(0, 8);

            await dbRun(
                `INSERT INTO messages (id, restaurant, table_number, message, icon, created_at, delivered)
                 VALUES (?, ?, ?, ?, ?, ?, 0)`,
                [id, resto, tKey, text, safeIcon, createdAt]
            );

            const room = `table_${resto}_${tKey}`;
            const roomSockets = io.sockets.adapter.rooms.get(room);
            const isLive = !!(roomSockets && roomSockets.size > 0);
            if (isLive) {
                io.to(room).emit('admin_message', { id, message: text, icon: safeIcon, createdAt });
                await dbRun('UPDATE messages SET delivered = 1 WHERE id = ?', [id]);
            }

            const subs = await getPushSubs(resto, tKey);
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
        } catch (e) {
            console.error(e);
            cb({ success: false, message: 'Server error' });
        }
    });
});

// ---------- REST APIs ----------
app.get('/api/restaurants', async (req, res) => {
    try {
        let files = fs.readdirSync(restaurantsDir).filter(f => f.endsWith('.json'));
        if (files.length === 0) {
            const rows = await dbAll('SELECT DISTINCT restaurant FROM menu_categories');
            if (rows.length > 0) {
                return res.json(rows.map(r => r.restaurant));
            }
            fs.writeFileSync(path.join(restaurantsDir, 'Default.json'), JSON.stringify([], null, 4));
            files = ['Default.json'];
        }
        res.json(files.map(f => path.basename(f, '.json')));
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Unable to scan restaurants directory' });
    }
});

// ---------- Admin login ----------
app.post('/api/admin-login', express.json(), (req, res) => {
    const { password } = req.body || {};
    if (!password) return res.status(400).json({ success: false, message: 'Password required' });

    if (String(password) !== ADMIN_PASSWORD) {
        return res.status(401).json({ success: false, message: 'Incorrect password' });
    }

    const token = 'adm_' + uuidv4().replace(/-/g, '') + Date.now().toString(36);
    adminSessions.set(token, { expiresAt: Date.now() + ADMIN_SESSION_MS });
    res.json({ success: true, token, expiresAt: Date.now() + ADMIN_SESSION_MS });
});

app.get('/api/admin-check', (req, res) => {
    const token = req.query.token || req.headers['x-admin-token'];
    if (!token) return res.status(401).json({ success: false });
    const rec = adminSessions.get(token);
    if (!rec || rec.expiresAt < Date.now()) {
        adminSessions.delete(token);
        return res.status(401).json({ success: false });
    }
    rec.expiresAt = Date.now() + ADMIN_SESSION_MS;
    res.json({ success: true, expiresAt: rec.expiresAt });
});

app.post('/api/admin-logout', express.json(), (req, res) => {
    const token = (req.body && req.body.token) || req.headers['x-admin-token'];
    if (token) adminSessions.delete(token);
    res.json({ success: true });
});

app.get('/api/qr-links', async (req, res) => {
    try {
        const targetResto = req.query.restaurant;
        let files = fs.readdirSync(restaurantsDir).filter(f => f.endsWith('.json'));
        let restaurants = files.map(f => path.basename(f, '.json'));
        if (targetResto) restaurants = restaurants.filter(r => r === targetResto);

        const allLinks = [];
        for (const resto of restaurants) {
            const restoTables = await dbAll('SELECT table_number FROM tables_state WHERE restaurant = ?', [resto]);
            restoTables.forEach(t => {
                const tableNumStr = String(t.table_number);
                const encodedTbl = Buffer.from(tableNumStr).toString('base64').split('').reverse().join('');
                const encodedResto = Buffer.from(resto).toString('base64').split('').reverse().join('');

                allLinks.push({
                    restaurant: resto, tableNumber: t.table_number,
                    encodedUrl: `/customer.html?tbl=${encodeURIComponent(encodedTbl)}&rst=${encodeURIComponent(encodedResto)}`
                });
            });
        }
        res.json(allLinks);
    } catch (e) {
        console.error('QR Links Error:', e);
        res.status(500).json({ error: 'Failed to generate QR links', details: e.message });
    }
});

app.post('/api/tables/:restaurantName', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const { tableNumber, location, capacity } = req.body;
        if (!tableNumber) return res.status(400).json({ success: false, message: 'Table number required' });

        const loc = location == null || location === '' ? null : String(location).slice(0, 40);
        const cap = capacity == null || capacity === '' ? null : Math.max(1, Math.min(50, parseInt(capacity) || 1));

        await dbRun(
            `INSERT INTO tables_state (restaurant, table_number, is_open, can_order, status, idle_minutes, location, capacity)
             VALUES (?, ?, 0, 0, 'Valid', 360, ?, ?)
             ON CONFLICT(restaurant, table_number) DO NOTHING`,
            [safeName, String(tableNumber), loc, cap]
        );
        await broadcastState();
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false });
    }
});

app.put('/api/tables/:restaurantName/:tableNumber', express.json(), async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const tKey = String(req.params.tableNumber);
        const updates = req.body || {};

        const existing = await dbGet(
            'SELECT 1 AS ok FROM tables_state WHERE restaurant = ? AND table_number = ?',
            [safeName, tKey]
        );
        if (!existing) return res.status(404).json({ success: false, message: 'Table not found' });

        const fields = [];
        const args = [];

        if (updates.location !== undefined) {
            const v = updates.location == null || updates.location === '' ? null : String(updates.location).slice(0, 40);
            fields.push('location = ?');
            args.push(v);
        }
        if (updates.capacity !== undefined) {
            const v = updates.capacity == null || updates.capacity === ''
                ? null
                : Math.max(1, Math.min(50, parseInt(updates.capacity) || 1));
            fields.push('capacity = ?');
            args.push(v);
        }
        if (updates.party_size !== undefined || updates.partySize !== undefined) {
            const raw = updates.party_size !== undefined ? updates.party_size : updates.partySize;
            const v = raw == null || raw === ''
                ? null
                : Math.max(0, Math.min(100, parseInt(raw) || 0));
            fields.push('party_size = ?');
            args.push(v);
        }

        if (fields.length === 0) return res.json({ success: true, noChanges: true });

        args.push(safeName, tKey);
        await dbRun(
            `UPDATE tables_state SET ${fields.join(', ')} WHERE restaurant = ? AND table_number = ?`,
            args
        );

        await broadcastState();
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: e.message });
    }
});

app.delete('/api/tables/:restaurantName/:tableNumber', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const tKey = String(req.params.tableNumber);
        await dbRun('DELETE FROM tables_state WHERE restaurant = ? AND table_number = ?', [safeName, tKey]);
        await dbRun('DELETE FROM table_requests WHERE restaurant = ? AND table_number = ?', [safeName, tKey]);
        await dbRun('DELETE FROM sessions WHERE restaurant = ? AND table_number = ?', [safeName, tKey]);
        await dbRun('DELETE FROM push_subs WHERE restaurant = ? AND table_number = ?', [safeName, tKey]);
        await dbRun('DELETE FROM messages WHERE restaurant = ? AND table_number = ?', [safeName, tKey]);
        await broadcastState();
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false });
    }
});

// ---------- Settings ----------
app.get('/api/settings/:restaurantName', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const rows = await dbAll('SELECT key, value FROM settings WHERE restaurant = ?', [safeName]);
        const out = {};
        rows.forEach(r => { out[r.key] = r.value; });
        if (out.show_photos == null) out.show_photos = '1';
        res.json(out);
    } catch (e) {
        console.error(e);
        res.status(500).json({});
    }
});

app.put('/api/settings/:restaurantName', express.json(), async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const updates = req.body || {};
        for (const [key, value] of Object.entries(updates)) {
            await dbRun(
                `INSERT INTO settings (restaurant, key, value) VALUES (?, ?, ?)
                 ON CONFLICT(restaurant, key) DO UPDATE SET value = excluded.value`,
                [safeName, String(key), String(value)]
            );
        }
        io.emit('settings_updated', { restaurant: safeName, settings: updates });
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: e.message });
    }
});

// ---------- Menu APIs ----------
app.get('/api/menu/:restaurantName', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const categories = await dbAll(
            'SELECT id, category_en, category_ar FROM menu_categories WHERE restaurant = ? ORDER BY id ASC',
            [safeName]
        );
        const items = await dbAll(
            'SELECT id, category_id, name_en, name_ar, price, desc_en, desc_ar, image, sort, dietary FROM menu_items WHERE restaurant = ? ORDER BY category_id, sort ASC, rowid ASC',
            [safeName]
        );

        const itemsByCat = {};
        items.forEach(it => {
            if (!itemsByCat[it.category_id]) itemsByCat[it.category_id] = [];
            let dietaryArr = [];
            try { dietaryArr = it.dietary ? JSON.parse(it.dietary) : []; } catch (e) { dietaryArr = []; }
            itemsByCat[it.category_id].push({
                id: it.id,
                nameEn: it.name_en,
                nameAr: it.name_ar,
                price: it.price,
                descEn: it.desc_en || '',
                descAr: it.desc_ar || '',
                image: it.image || undefined,
                sort: it.sort == null ? 0 : Number(it.sort),
                dietary: dietaryArr,
            });
        });

        const menu = categories.map(c => ({
            categoryEn: c.category_en,
            categoryAr: c.category_ar || c.category_en,
            items: itemsByCat[c.id] || [],
        }));

        res.json(menu);
    } catch (e) {
        console.error(e);
        res.status(500).json([]);
    }
});

// ---------- Preview endpoint ----------
app.get('/api/preview/:restaurantName', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const tableNumber = String(req.query.table || '');

        const categories = await dbAll(
            'SELECT id, category_en, category_ar FROM menu_categories WHERE restaurant = ? ORDER BY id ASC',
            [safeName]
        );
        const items = await dbAll(
            'SELECT id, category_id, name_en, name_ar, price, desc_en, desc_ar, image, sort, dietary FROM menu_items WHERE restaurant = ? ORDER BY category_id, sort ASC, rowid ASC',
            [safeName]
        );

        const itemsByCat = {};
        items.forEach(it => {
            if (!itemsByCat[it.category_id]) itemsByCat[it.category_id] = [];
            let dietaryArr = [];
            try { dietaryArr = it.dietary ? JSON.parse(it.dietary) : []; } catch (e) { dietaryArr = []; }
            itemsByCat[it.category_id].push({
                id: it.id,
                nameEn: it.name_en,
                nameAr: it.name_ar,
                price: it.price,
                descEn: it.desc_en || '',
                descAr: it.desc_ar || '',
                image: it.image || undefined,
                sort: it.sort == null ? 0 : Number(it.sort),
                dietary: dietaryArr,
            });
        });

        const menu = categories.map(c => ({
            categoryEn: c.category_en,
            categoryAr: c.category_ar || c.category_en,
            items: itemsByCat[c.id] || [],
        }));

        let tableState = null;
        if (tableNumber) {
            const row = await dbGet(
                'SELECT is_open, can_order, status, idle_minutes, location, capacity, party_size FROM tables_state WHERE restaurant = ? AND table_number = ?',
                [safeName, tableNumber]
            );
            if (row) {
                tableState = {
                    isOpen: !!row.is_open,
                    canOrder: !!row.can_order,
                    status: row.status,
                    idleMinutes: (row.idle_minutes == null) ? 360 : row.idle_minutes,
                    location: row.location || null,
                    capacity: (row.capacity == null) ? null : Number(row.capacity),
                    partySize: (row.party_size == null) ? null : Number(row.party_size),
                };
            }
        }

        const sessionKey = `${safeName}_${tableNumber}`;
        const guestsConnected = (tableSockets[sessionKey] && tableSockets[sessionKey].size) || 0;

        const settingsRows = await dbAll('SELECT key, value FROM settings WHERE restaurant = ?', [safeName]);
        const settings = {};
        settingsRows.forEach(r => { settings[r.key] = r.value; });
        if (settings.show_photos == null) settings.show_photos = '1';

        res.json({
            success: true,
            restaurant: safeName,
            tableNumber,
            menu,
            table: tableState,
            guestsConnected,
            settings,
        });
    } catch (e) {
        console.error('Preview error:', e);
        res.status(500).json({ success: false, message: e.message });
    }
});

app.post('/api/menu/:restaurantName', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const { category, nameEn, nameAr, price, descEn, descAr, dietary } = req.body;
        if (!category || !nameEn) return res.status(400).json({ success: false, message: 'Missing fields' });

        let cat = await dbGet(
            'SELECT id FROM menu_categories WHERE restaurant = ? AND category_en = ?',
            [safeName, category]
        );
        if (!cat) {
            const r = await dbRun(
                'INSERT INTO menu_categories (restaurant, category_en, category_ar) VALUES (?, ?, ?)',
                [safeName, category, category]
            );
            cat = { id: Number(r.lastInsertRowid) };
        }

        const newId = uuidv4();
        const maxRow = await dbGet(
            'SELECT COALESCE(MAX(sort), 0) AS m FROM menu_items WHERE restaurant = ? AND category_id = ?',
            [safeName, cat.id]
        );
        const nextSort = (maxRow && maxRow.m ? Number(maxRow.m) : 0) + 10;

        const dietaryJson = Array.isArray(dietary) ? JSON.stringify(dietary) : '[]';

        await dbRun(
            `INSERT INTO menu_items (id, restaurant, category_id, name_en, name_ar, price, desc_en, desc_ar, image, sort, dietary)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
            [
                newId, safeName, cat.id, nameEn, nameAr || nameEn,
                parseFloat(price) || 0, descEn || '', descAr || '',
                nextSort, dietaryJson
            ]
        );

        await broadcastState();
        io.emit('menu_updated', { restaurant: safeName });
        res.json({ success: true, item: { id: newId } });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: e.message });
    }
});

app.put('/api/menu/:restaurantName/:itemId', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const itemId = req.params.itemId;
        const { category, nameEn, nameAr, price, descEn, descAr, dietary, sort } = req.body;

        const existing = await dbGet(
            'SELECT id, category_id FROM menu_items WHERE restaurant = ? AND id = ?',
            [safeName, itemId]
        );
        if (!existing) return res.status(404).json({ success: false, message: 'Item not found' });

        let cat = await dbGet(
            'SELECT id FROM menu_categories WHERE restaurant = ? AND category_en = ?',
            [safeName, category]
        );
        if (!cat) {
            const r = await dbRun(
                'INSERT INTO menu_categories (restaurant, category_en, category_ar) VALUES (?, ?, ?)',
                [safeName, category, category]
            );
            cat = { id: Number(r.lastInsertRowid) };
        }

        const dietaryJson = Array.isArray(dietary) ? JSON.stringify(dietary) : '[]';

        let finalSort = sort;
        if (finalSort == null || finalSort === '') {
            const sortRow = await dbGet(
                'SELECT sort FROM menu_items WHERE restaurant = ? AND id = ?',
                [safeName, itemId]
            );
            finalSort = sortRow && sortRow.sort != null ? Number(sortRow.sort) : 0;
        } else {
            finalSort = Number(finalSort) || 0;
        }

        await dbRun(
            `UPDATE menu_items
             SET category_id = ?, name_en = ?, name_ar = ?, price = ?, desc_en = ?, desc_ar = ?, sort = ?, dietary = ?
             WHERE restaurant = ? AND id = ?`,
            [
                cat.id, nameEn, nameAr || nameEn, parseFloat(price) || 0, descEn || '', descAr || '',
                finalSort, dietaryJson, safeName, itemId
            ]
        );

        await broadcastState();
        io.emit('menu_updated', { restaurant: safeName });
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: e.message });
    }
});

app.post('/api/menu/:restaurantName/reorder', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const { category, orderedIds } = req.body;
        if (!category || !Array.isArray(orderedIds)) {
            return res.status(400).json({ success: false, message: 'category and orderedIds required' });
        }

        const catRow = await dbGet(
            'SELECT id FROM menu_categories WHERE restaurant = ? AND category_en = ?',
            [safeName, category]
        );
        if (!catRow) return res.status(404).json({ success: false, message: 'Category not found' });

        for (let i = 0; i < orderedIds.length; i++) {
            await dbRun(
                'UPDATE menu_items SET sort = ? WHERE restaurant = ? AND id = ? AND category_id = ?',
                [(i + 1) * 10, safeName, orderedIds[i], catRow.id]
            );
        }

        await broadcastState();
        io.emit('menu_updated', { restaurant: safeName });
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: e.message });
    }
});

app.delete('/api/menu/:restaurantName/:itemId', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const itemId = req.params.itemId;
        await dbRun('DELETE FROM menu_items WHERE restaurant = ? AND id = ?', [safeName, itemId]);

        const emptyCats = await dbAll(
            `SELECT c.id FROM menu_categories c
             WHERE c.restaurant = ?
             AND NOT EXISTS (SELECT 1 FROM menu_items i WHERE i.category_id = c.id)`,
            [safeName]
        );
        for (const c of emptyCats) {
            await dbRun('DELETE FROM menu_categories WHERE id = ?', [c.id]);
        }

        await broadcastState();
        io.emit('menu_updated', { restaurant: safeName });
        res.json({ success: true });
    } catch (e) {
        console.error(e);
        res.status(500).json({ success: false, message: e.message });
    }
});

// ---------- Photo Upload (R2) ----------
app.post('/api/menu/:restaurantName/:itemId/photo', upload.single('photo'), async (req, res) => {
    if (!req.file) return res.status(400).json({ success: false, message: 'No file uploaded' });

    try {
        const safeName = path.basename(req.params.restaurantName);
        const itemId = String(req.params.itemId).replace(/[^a-zA-Z0-9_-]/g, '');
        const ext = path.extname(req.file.originalname).toLowerCase() || '.jpg';
        const key = `dish-photos/${itemId}${ext}`;

        await r2Client.send(new PutObjectCommand({
            Bucket: process.env.R2_BUCKET_NAME,
            Key: key,
            Body: req.file.buffer,
            ContentType: req.file.mimetype,
        }));

        const publicUrl = `${process.env.R2_PUBLIC_URL}/${key}`;

        await dbRun(
            'UPDATE menu_items SET image = ? WHERE restaurant = ? AND id = ?',
            [publicUrl, safeName, itemId]
        );

        await broadcastState();
        io.emit('menu_updated', { restaurant: safeName });
        res.json({ success: true, url: publicUrl });
    } catch (err) {
        console.error('R2 Upload Error:', err);
        res.status(500).json({ success: false, message: 'Upload to R2 failed: ' + err.message });
    }
});

app.delete('/api/menu/:restaurantName/:itemId/photo', async (req, res) => {
    try {
        const safeName = path.basename(req.params.restaurantName);
        const itemId = String(req.params.itemId).replace(/[^a-zA-Z0-9_-]/g, '');

        const exts = ['.jpg', '.jpeg', '.png', '.webp'];
        await Promise.allSettled(exts.map(ext =>
            r2Client.send(new DeleteObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: `dish-photos/${itemId}${ext}`,
            }))
        ));

        await dbRun(
            'UPDATE menu_items SET image = NULL WHERE restaurant = ? AND id = ?',
            [safeName, itemId]
        );

        await broadcastState();
        io.emit('menu_updated', { restaurant: safeName });
        res.json({ success: true });
    } catch (err) {
        console.error('R2 Delete Error:', err);
        res.status(500).json({ success: false, message: err.message });
    }
});

app.get('/api/revenue', async (req, res) => {
    try {
        const history = await dbAll('SELECT * FROM paid_history');
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
    } catch (e) {
        console.error(e);
        res.status(500).json({});
    }
});

// ---------- Boot ----------
const PORT = process.env.PORT || 3000;

(async () => {
    try {
        await ensureSchema();
        server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
    } catch (e) {
        console.error('❌ Failed to start:', e);
        process.exit(1);
    }
})();