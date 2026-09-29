// ═══════════════════════════════════════════════════════════════════════════════
// DELETED SIGNAL DETECTOR v2.2
// Telegram → per-source SQLite + deleted logs + live status channel
//
// HOW IT WORKS
//   1. At startup, the most recent messages per source are cached in a
//      per-source DB (db/<source>.db) so deletions of pre-start messages
//      can still be recovered.
//   2. Every incoming text message is cached the moment it arrives.
//   3. When Telegram reports a deletion, the cached text is recovered by ID
//      and logged instantly to logs/<source>_deleted.log + Telegram channel.
//      Detected rows are kept in the DB (deleted=1) as an archive.
//
// FORWARDED-MESSAGE FILTER
//   · Messages forwarded from a DIFFERENT source are never cached.
//   · Messages forwarded from the SAME channel are cached normally.
//   · Hidden/anonymous forwards (no fromId) are treated as "different source".
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

// Forwarded-message filter:
//   true  → a message forwarded from a DIFFERENT source is skipped (never cached)
//   same-source forwards are still cached unless ALLOW_SAME_SOURCE_FORWARD = false
const IGNORE_FORWARDED_FROM_OTHER_SOURCES = true;
const ALLOW_SAME_SOURCE_FORWARD = true;

// Heartbeat: how often the channel About panel is refreshed.
// 1 minute is well under Telegram's FloodWait threshold for EditAbout.
const STATUS_HEARTBEAT_MS = 60 * 1000;

const CONFIG_FILE = 'config.json';
const DB_DIR = 'db';
const LOGS_DIR = 'logs';

const defaultConfig = {
    telegramSources: [],    // channel/group IDs or @usernames
    logChannel: ''          // channel ID/@username where deletions are posted + status About is maintained
};

let config = {};
let telegramClient = null;

// sourceKey (normalized bare id) → { key, entity, title, db }
const sources = new Map();

// ─── Status channel state ───
let statusPeer = null;              // resolved InputPeer for the log channel
let statusLastEditAt = 0;           // last successful About edit (ms)
let statusFloodWaitUntil = 0;       // skip edits until this timestamp
let statusLastError = '';
const processStartedAt = Date.now();

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
        // Allow LOG_CHANNEL env var to override config.json
        if (process.env.LOG_CHANNEL && !config.logChannel) {
            config.logChannel = process.env.LOG_CHANNEL;
        }
        return config;
    } catch (err) {
        console.error(`[${getTimestamp()}][CONFIG] Error loading config:`, err.message);
        return defaultConfig;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FORWARDED-MESSAGE FILTER
// ═══════════════════════════════════════════════════════════════════════════════

// Returns true if this message should be SKIPPED because it was forwarded
// from a different source than the channel it was posted in.
// Shared by the startup seed and the live message cache.
function isForwardedFromElsewhere(msg) {
    if (!IGNORE_FORWARDED_FROM_OTHER_SOURCES || !msg.fwdFrom) return false;

    const fwdPeer = msg.fwdFrom.fromId;
    let fwdSourceId = null;

    if (fwdPeer) {
        if (fwdPeer.className === 'PeerChannel') fwdSourceId = fwdPeer.channelId;
        else if (fwdPeer.className === 'PeerChat') fwdSourceId = fwdPeer.chatId;
        else if (fwdPeer.className === 'PeerUser') fwdSourceId = fwdPeer.userId; // user ≠ channel → different source
    }

    // fwdSourceId null (hidden/anonymous forward) → cannot verify origin → treat as different source
    const sameSource = fwdSourceId != null && normKey(fwdSourceId) === normKey(msg.chatId);

    return !sameSource || !ALLOW_SAME_SOURCE_FORWARD;
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

    // ─── NEW v2.2: also push to the Telegram log channel ───
    pushDeletedToChannel(ctx, messageId, text, date);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TELEGRAM LOG CHANNEL (v2.2)
// ═══════════════════════════════════════════════════════════════════════════════

// Formatting helpers mirroring the Logger example style
function fmtTimeOnly(date = new Date()) {
    const hours = date.getHours();
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const subscripts = ['₀', '₁', '₂', '₃', '₄', '₅', '₆', '₇', '₈', '₉'];
    const seconds = String(date.getSeconds())
        .padStart(2, '0')
        .replace(/\d/g, d => subscripts[d]);
    return `${hours}:${minutes}:${seconds}`;
}

// yyyy-mm-dd hh:mm (24h) — used for the status panel's Last Active line
function fmtDateTime(date = new Date()) {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const mi = String(date.getMinutes()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}

function formatUptime(ms) {
    const totalSec = Math.floor(ms / 1000);
    const d = Math.floor(totalSec / 86400);
    const h = Math.floor((totalSec % 86400) / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    const pad = n => String(n).padStart(2, '0');
    return `${pad(d)}d ${pad(h)}h ${pad(m)}m ${pad(s)}s`;
}

async function resolveLogChannel() {
    if (!config.logChannel) {
        console.log(`[${getTimestamp()}][STATUS] No logChannel configured — Telegram logging disabled (set LOG_CHANNEL or config.logChannel)`);
        return;
    }
    try {
        statusPeer = await telegramClient.getInputEntity(config.logChannel);
        console.log(`[${getTimestamp()}][STATUS] Log channel: ${config.logChannel}`);
    } catch (err) {
        console.error(`[${getTimestamp()}][STATUS] Could not resolve log channel '${config.logChannel}':`, err.message);
        statusPeer = null;
    }
}

// Post a recovered deleted message to the log channel
async function pushDeletedToChannel(ctx, messageId, text, date) {
    if (!statusPeer) return;

    const truncated = text.length > 3500 ? text.substring(0, 3500) + '\n\n…[truncated]' : text;
    const message =
        `🗑️ **DELETED MESSAGE DETECTED**\n` +
        `→ Source: ${ctx.title}\n` +
        `→ Message ID: ${messageId}\n` +
        `→ Originally posted: ${date}\n\n` +
        `${truncated}\n\n` +
        `@ ${fmtTimeOnly()}`;

    try {
        await telegramClient.sendMessage(statusPeer, { message });
    } catch (err) {
        console.error(`[${getTimestamp()}][ERROR] Failed to post deletion to log channel:`, err.message);
    }
}

// ─── Live status panel: channel About (description) heartbeat ───
const ABOUT_MAX_LEN = 255;   // Telegram channel description hard limit

function buildStatusAbout() {
    const onOff = v => v ? 'on' : 'off';

    const base = () =>
        `SIGNAL DELETION DETECTOR\n` +
        (statusLastError ? `⚠️ ${statusLastError}\n` : '') +
        `Last Active: ${fmtDateTime()}\n` +
        `Sources: ${sources.size}\n` +
        `DEBUG=${onOff(DEBUG)} · Seed=${SEED_LIMIT} · Forward Filter=${onOff(IGNORE_FORWARDED_FROM_OTHER_SOURCES)}`;

    let about = base();
    if (about.length > ABOUT_MAX_LEN) {
        about = about.slice(0, ABOUT_MAX_LEN - 1) + '…';
    }
    return about;
}

function getEditAboutCtor() {
    const msgs = Api.messages || {};
    const chans = Api.channels || {};
    return msgs.EditChatAbout || msgs.editChatAbout ||
        chans.EditAbout || chans.editAbout || null;
}

let editAboutLoggedMissing = false;

async function updateStatusAbout() {
    if (!statusPeer) return;
    if (Date.now() < statusFloodWaitUntil) return;   // Telegram told us to wait

    const EditAbout = getEditAboutCtor();
    if (!EditAbout) {
        if (!editAboutLoggedMissing) {
            console.error(`[${getTimestamp()}][ERROR] No EditChatAbout/EditAbout constructor found. ` +
                `Try updating gramJS: npm i telegram@latest`);
            editAboutLoggedMissing = true;
        }
        return;
    }

    const about = buildStatusAbout();
    try {
        await telegramClient.invoke(
            new EditAbout({ peer: statusPeer, about })
        );
        statusLastEditAt = Date.now();
        statusLastError = '';
    } catch (err) {
        // FloodWait (420) → back off for the requested duration
        if (err && err.code === 420 && err.seconds) {
            statusFloodWaitUntil = Date.now() + err.seconds * 1000;
            statusLastError = `FloodWait ${err.seconds}s`;
            if (DEBUG) console.log(`[${getTimestamp()}][STATUS] FloodWait — pausing About edits for ${err.seconds}s`);
        } else if (err && err.errorMessage === 'CHAT_ABOUT_NOT_MODIFIED') {
            statusLastEditAt = Date.now();   // text unchanged — still alive
        } else {
            statusLastError = err.message;
            console.error(`[${getTimestamp()}][ERROR] Status About update failed:`, err.message);
        }
    }
}

let statusHeartbeatTimer = null;

function startStatusHeartbeat() {
    if (!config.logChannel) return;
    statusHeartbeatTimer = setInterval(() => {
        updateStatusAbout().catch(() => { });
    }, STATUS_HEARTBEAT_MS);
    console.log(`[${getTimestamp()}][STATUS] Heartbeat started (${STATUS_HEARTBEAT_MS}ms interval)`);
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
            if (isForwardedFromElsewhere(msg)) continue;
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

        // ─── Resolve log channel BEFORE seeding (so deletions can be posted) ───
        await resolveLogChannel();

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

            if (isForwardedFromElsewhere(msg)) {
                if (DEBUG) console.log(`[${getTimestamp()}][CACHE] Skipped forwarded-from-elsewhere message id=${msg.id} in chat ${normKey(msg.chatId)}`);
                return;
            }

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

        // ─── Start the channel-description heartbeat ───
        startStatusHeartbeat();

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
    if (statusHeartbeatTimer) clearInterval(statusHeartbeatTimer);
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
    console.log('║     DELETED SIGNAL DETECTOR v2.2           ║');
    console.log('║     Instant deletion recovery              ║');
    console.log('║     Per-source SQLite · logs/ output       ║');
    console.log('╚════════════════════════════════════════════╝\n');

    ensureConfigExists();
    loadConfig();
    ensureDirs();

    console.log(`[${getTimestamp()}][SYSTEM] Sources configured: ${config.telegramSources.length}`);
    console.log(`[${getTimestamp()}][SYSTEM] Log channel: ${config.logChannel || '(not set — Telegram logging disabled)'}\n`);

    await connectTelegram();

    console.log(`\n[${getTimestamp()}][SYSTEM] ✅ Running. Deletions will be logged instantly.\n`);
})();
