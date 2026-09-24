// ═══════════════════════════════════════════════════════════════════════════════
// DELETED SIGNAL DETECTOR v2.0 — FINAL
// Telegram → per-source SQLite + deleted logs
//
// HOW IT WORKS
//   1. At startup, the most recent messages per source are cached in a
//      per-source DB (db/<source>.db) so deletions of pre-start messages
//      can still be recovered.
//   2. Every incoming text message is cached the moment it arrives.
//   3. When Telegram reports a deletion, the cached text is recovered by ID
//      and logged instantly to logs/<source>_deleted.log with the original
//      post date. Detected rows are kept in the DB (deleted=1) as an archive.
//
// NOTES
//   · Telegram deletion updates carry NO text — the DB is the only source of
//     recovered content. Textless messages (service/media) are never cached.
//   · gramJS passes the RAW UPDATE itself as the handler argument (no wrapper).
//   · IDs are normalized to bare form everywhere (entity ids and msg.chatId
//     use different dialects; see normKey).
// ═══════════════════════════════════════════════════════════════════════════════

const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions/index.js');
const { NewMessage, Raw } = require('telegram/events/index.js');
const { Api } = require('telegram/tl/api');

dotenv.config();

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════

const DEBUG = false;        // verbose logging (cached messages, raw updates)
const SEED_LIMIT = 50;      // messages to snapshot per source at startup

const CONFIG_FILE = 'config.json';
const DB_DIR = 'db';
const LOGS_DIR = 'logs';

const defaultConfig = {
    telegramSources: []     // channel/group IDs or @usernames
};

let config = {};
let telegramClient = null;

// sourceKey (normalized bare id) → { key, entity, title, db }
const sources = new Map();

// ═══════════════════════════════════════════════════════════════════════════════
// ID NORMALIZATION — Telegram IDs come in two dialects:
//   bare:      2401234567   (entity.id, UpdateDeleteChannelMessages.channelId)
//   marked:    -1002401234567  (msg.chatId)
// All keys are stored and looked up in bare form.
// ═══════════════════════════════════════════════════════════════════════════════

function normKey(idLike) {
    const s = idLike.toString();
    if (s.startsWith('-100')) return s.slice(4);
    if (s.startsWith('-')) return s.slice(1);
    return s;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════════════════════════

function ensureConfigExists() {
    try {
        fs.accessSync(CONFIG_FILE, fs.constants.F_OK);
    } catch {
        fs.writeFileSync(CONFIG_FILE, JSON.stringify(defaultConfig, null, 2), 'utf8');
        console.log(`[${getTimestamp()}][CONFIG] Created default ${CONFIG_FILE}`);
    }
}

function loadConfig() {
    try {
        const data = fs.readFileSync(CONFIG_FILE, 'utf8');
        config = { ...defaultConfig, ...JSON.parse(data) };
        return config;
    } catch (err) {
        console.error(`[${getTimestamp()}][CONFIG] Error loading config:`, err.message);
        return defaultConfig;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STORAGE & LOGGING
// ═══════════════════════════════════════════════════════════════════════════════

function ensureDirs() {
    for (const dir of [DB_DIR, LOGS_DIR]) {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(`[${getTimestamp()}][SYSTEM] Created directory: ${dir}`);
        }
    }
}

function sanitizeFilename(name) {
    return name
        .replace(/[<>:"/\\|?*]/g, '_')
        .replace(/\s+/g, '_')
        .trim()
        .substring(0, 100);
}

// ─── Per-source context: one SQLite file per source ───
function openSourceDb(title) {
    const db = new DatabaseSync(path.join(DB_DIR, `${sanitizeFilename(title)}.db`));
    db.exec('PRAGMA journal_mode = WAL');
    db.exec(`
        CREATE TABLE IF NOT EXISTS messages (
            message_id   INTEGER PRIMARY KEY,
            text         TEXT NOT NULL,
            date         TEXT NOT NULL,
            deleted      INTEGER NOT NULL DEFAULT 0,
            detected_via TEXT NOT NULL DEFAULT 'event'
        )
    `);
    return db;
}

function insertMessage(ctx, messageId, text, via) {
    const info = ctx.db.prepare(
        'INSERT OR IGNORE INTO messages (message_id, text, date, deleted, detected_via) VALUES (?, ?, ?, 0, ?)'
    ).run(messageId, text, getTimestamp(), via);
    if (info.changes > 0 && DEBUG) {
        console.log(`[${getTimestamp()}][CACHE] ${ctx.title} ← id=${messageId} via=${via}`);
    }
    return info.changes > 0;
}

function markDeleted(ctx, messageId, via) {
    return ctx.db.prepare(
        'UPDATE messages SET deleted = 1, detected_via = ? WHERE message_id = ? AND deleted = 0'
    ).run(via, messageId).changes > 0;
}

function getCachedMessage(ctx, messageId) {
    return ctx.db.prepare(
        'SELECT text, date FROM messages WHERE message_id = ?'
    ).get(messageId);
}

function logDeleted(ctx, messageId, text, date) {
    const logFile = path.join(LOGS_DIR, `${sanitizeFilename(ctx.title)}_deleted.log`);
    const entry =
        `[${getTimestamp()}] DELETED MESSAGE DETECTED (posted ${date}):\n` +
        `${text}\n${'-'.repeat(40)}\n`;
    try {
        fs.appendFileSync(logFile, entry, 'utf8');
    } catch (err) {
        console.error(`[${getTimestamp()}][ERROR] Failed to write deleted log:`, err.message);
    }
    const preview = text.split('\n')[0].substring(0, 60);
    console.log(`[${getTimestamp()}][DETECTOR] 🗑️  ${ctx.title} — deleted id=${messageId}: "${preview}"`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SOURCE REGISTRY
// ═══════════════════════════════════════════════════════════════════════════════

function registerSource(idLike, entity, title) {
    const key = normKey(idLike);
    if (sources.has(key)) return sources.get(key);
    const ctx = { key, entity, title, db: openSourceDb(title) };
    sources.set(key, ctx);
    console.log(`[${getTimestamp()}][DETECTOR] Tracking: ${title}`);
    return ctx;
}

async function resolveSourceName(entityLike) {
    try {
        const entity = await telegramClient.getEntity(entityLike);
        return {
            entity,
            title: entity.title || entity.firstName || entity.username || String(entityLike)
        };
    } catch {
        return null;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DELETION HANDLERS
// ═══════════════════════════════════════════════════════════════════════════════

function handleDeletedIds(ctx, ids) {
    for (const id of ids) {
        const cached = getCachedMessage(ctx, id);
        if (!cached) {
            if (DEBUG) console.log(`[${getTimestamp()}][DETECTOR] id=${id} deleted in ${ctx.title} but not cached — nothing to recover`);
            continue;
        }
        if (!markDeleted(ctx, id, 'event')) continue;  // already handled
        logDeleted(ctx, id, cached.text, cached.date);
    }
}

// Channels / supergroups: update carries channelId + message ids
async function onChannelMessagesDeleted(update) {
    const key = normKey(update.channelId);
    let ctx = sources.get(key);
    if (!ctx) {
        const resolved = await resolveSourceName(update.channelId);
        if (!resolved) {
            if (DEBUG) console.log(`[${getTimestamp()}][DETECTOR] Delete update for untracked channelId=${key}`);
            return;
        }
        ctx = registerSource(key, resolved.entity, resolved.title);
    }
    handleDeletedIds(ctx, update.messages.map(Number));
}

// Plain chats / groups: update carries ONLY message ids, no chat id.
// Search every tracked source DB for a match.
function onPlainMessagesDeleted(update) {
    const ids = update.messages.map(Number);
    for (const ctx of sources.values()) {
        const found = ids.filter(id => getCachedMessage(ctx, id));
        if (found.length) handleDeletedIds(ctx, found);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STARTUP SEED — snapshot recent messages so pre-start deletions are recoverable
// ═══════════════════════════════════════════════════════════════════════════════

async function seedSource(ctx) {
    try {
        const history = await telegramClient.getMessages(ctx.entity, { limit: SEED_LIMIT });
        let count = 0;
        for (const msg of history) {
            if (!msg.message) continue;
            if (insertMessage(ctx, msg.id, msg.message, 'seed')) count++;
        }
        console.log(`[${getTimestamp()}][DETECTOR]   └ seeded ${count} cached message(s)`);
    } catch (err) {
        console.error(`[${getTimestamp()}][DETECTOR] Seed failed for ${ctx.title}:`, err.message);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TELEGRAM CLIENT
// ═══════════════════════════════════════════════════════════════════════════════

async function connectTelegram() {
    const apiId = parseInt(process.env.API_ID);
    const apiHash = process.env.API_HASH;
    const sessionString = process.env.STRING_SESSION || '';

    if (!sessionString) {
        console.error(`[${getTimestamp()}][TELEGRAM] STRING_SESSION not found in .env!`);
        process.exit(1);
    }

    telegramClient = new TelegramClient(new StringSession(sessionString), apiId, apiHash, {
        connectionRetries: 5,
        useWSS: true,
        useIPv6: false,
    });
    telegramClient.setLogLevel('none');

    try {
        await telegramClient.connect();
        const me = await telegramClient.getMe();
        console.log(`[${getTimestamp()}][TELEGRAM] ✅ Connected as @${me.username || me.firstName}`);

        // ─── Register configured sources & seed recent history ───
        for (const src of config.telegramSources) {
            const resolved = await resolveSourceName(src);
            if (!resolved) {
                console.error(`[${getTimestamp()}][CONFIG] Could not resolve source: ${src}`);
                continue;
            }
            const ctx = registerSource(resolved.entity.id ?? src, resolved.entity, resolved.title);
            await seedSource(ctx);
        }

        // ─── New messages → cache (text only) ───
        telegramClient.addEventHandler(async (event) => {
            const msg = event.message;
            if (!msg || !msg.message) return;
            if (!msg.chatId) return;

            const key = normKey(msg.chatId);
            let ctx = sources.get(key);
            if (!ctx) {
                const resolved = await resolveSourceName(msg.peerId);
                if (!resolved) return;
                ctx = registerSource(key, resolved.entity, resolved.title);
            }

            insertMessage(ctx, msg.id, msg.message, 'event');
        }, new NewMessage({}));

        // ─── Deletion updates (gramJS passes the raw update itself — no wrapper) ───
        telegramClient.addEventHandler(async (event) => {
            try {
                const update = (event && event.update) ? event.update : event;
                if (update instanceof Api.UpdateDeleteChannelMessages) {
                    if (DEBUG) {
                        console.log(`[${getTimestamp()}][RAW] UpdateDeleteChannelMessages ids=[${update.messages.join(',')}] channelId=${update.channelId}`);
                    }
                    await onChannelMessagesDeleted(update);
                } else if (update instanceof Api.UpdateDeleteMessages) {
                    if (DEBUG) {
                        console.log(`[${getTimestamp()}][RAW] UpdateDeleteMessages ids=[${update.messages.join(',')}]`);
                    }
                    onPlainMessagesDeleted(update);
                }
            } catch (err) {
                console.error(`[${getTimestamp()}][ERROR] Raw handler:`, err.message);
            }
        }, new Raw({
            types: [
                Api.UpdateDeleteMessages,
                Api.UpdateDeleteChannelMessages,
            ],
        }));

        console.log(`[${getTimestamp()}][DETECTOR] Watching ${sources.size} source(s) — instant mode, no polling.`);

    } catch (err) {
        console.error(`[${getTimestamp()}][TELEGRAM] Connection failed:`, err.message);
        setTimeout(connectTelegram, 10000);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════

function getTimestamp(date = new Date()) {
    const dd = String(date.getDate()).padStart(2, '0');
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const yyyy = date.getFullYear();
    let hours = date.getHours();
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12;
    const hh = String(hours).padStart(2, '0');
    return `${dd}-${mm}-${yyyy} ${hh}:${minutes}:${seconds} ${ampm}`;
}

function cleanup() {
    console.log(`\n[${getTimestamp()}][SYSTEM] Shutting down gracefully...`);
    for (const ctx of sources.values()) ctx.db.close();
    if (telegramClient) telegramClient.disconnect();
    process.exit(0);
}

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

(async () => {
    console.log('╔════════════════════════════════════════════╗');
    console.log('║     DELETED SIGNAL DETECTOR v2.0           ║');
    console.log('║     Instant deletion recovery              ║');
    console.log('║     Per-source SQLite · logs/ output       ║');
    console.log('╚════════════════════════════════════════════╝\n');

    ensureConfigExists();
    loadConfig();
    ensureDirs();

    console.log(`[${getTimestamp()}][SYSTEM] Sources configured: ${config.telegramSources.length}\n`);

    await connectTelegram();

    console.log(`\n[${getTimestamp()}][SYSTEM] ✅ Running. Deletions will be logged instantly.\n`);
})();
