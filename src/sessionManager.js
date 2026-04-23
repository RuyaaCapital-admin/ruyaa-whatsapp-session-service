import fs from 'node:fs/promises';
import path from 'node:path';

import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  jidNormalizedUser,
  useMultiFileAuthState,
} from '@whiskeysockets/baileys';
import pino from 'pino';
import QRCode from 'qrcode';
import { v4 as uuidv4 } from 'uuid';

import { sendWebhook } from './webhook.js';

const sessions = new Map();
const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
const MAX_MESSAGE_CACHE = Number(process.env.MAX_MESSAGE_CACHE || 2000);

function nowIso() {
  return new Date().toISOString();
}

function getSessionsDir() {
  return process.env.SESSIONS_DIR || path.resolve('data/sessions');
}

function extractPhoneFromJid(jid) {
  if (!jid) return null;
  return jid.split(':')[0].split('@')[0] || null;
}

function normalizeRecipient(to) {
  if (!to) return null;
  if (to.includes('@')) return to;
  const cleaned = String(to).replace(/[^\d]/g, '');
  return `${cleaned}@s.whatsapp.net`;
}

function getMessageText(message) {
  return (
    message?.conversation ||
    message?.extendedTextMessage?.text ||
    message?.imageMessage?.caption ||
    message?.videoMessage?.caption ||
    message?.documentMessage?.caption ||
    message?.buttonsResponseMessage?.selectedDisplayText ||
    message?.listResponseMessage?.title ||
    message?.templateButtonReplyMessage?.selectedDisplayText ||
    ''
  );
}

function getMessageType(message) {
  if (!message || typeof message !== 'object') return 'unknown';

  if (message.ephemeralMessage?.message) {
    return getMessageType(message.ephemeralMessage.message);
  }

  if (message.viewOnceMessage?.message) {
    return getMessageType(message.viewOnceMessage.message);
  }

  const keys = Object.keys(message);
  return keys[0] || 'unknown';
}

function getTimestampValue(value) {
  if (value == null) return null;
  if (typeof value === 'number') return value;
  if (typeof value === 'bigint') return Number(value);
  if (typeof value?.toNumber === 'function') return value.toNumber();
  if (typeof value?.low === 'number') return value.low;

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function timestampToIso(value) {
  const numeric = getTimestampValue(value);
  if (!numeric) return null;

  const milliseconds = numeric > 10_000_000_000 ? numeric : numeric * 1000;
  return new Date(milliseconds).toISOString();
}

function normalizeContact(contact = {}) {
  const jid = contact.id || contact.jid || null;
  const phoneNumber = extractPhoneFromJid(jid);
  const displayName = contact.notify || contact.name || contact.verifiedName || contact.pushName || phoneNumber || jid || 'Unknown';

  return {
    id: jid || phoneNumber,
    jid,
    phoneNumber,
    displayName,
    pushName: contact.notify || contact.pushName || null,
    verifiedName: contact.verifiedName || null,
    shortName: contact.short || null,
    isBusiness: Boolean(contact.verifiedName),
    updatedAt: nowIso(),
  };
}

function normalizeChat(chat = {}) {
  const jid = chat.id || null;
  const conversationTimestamp = chat.conversationTimestamp || chat.lastMessageRecvTimestamp || null;

  return {
    id: jid,
    jid,
    name: chat.name || chat.subject || extractPhoneFromJid(jid) || jid || 'Unknown chat',
    unreadCount: chat.unreadCount || 0,
    archived: Boolean(chat.archive),
    pinned: Boolean(chat.pinned),
    muteEndTime: getTimestampValue(chat.muteEndTime) || null,
    lastMessageAt: timestampToIso(conversationTimestamp),
    conversationTimestamp: getTimestampValue(conversationTimestamp),
    updatedAt: nowIso(),
  };
}

function normalizeMessage(message = {}) {
  const remoteJid = message?.key?.remoteJid || null;
  const participant = message?.key?.participant || null;
  const from = jidNormalizedUser(participant || remoteJid || '');
  const providerMessageId = message?.key?.id || `${remoteJid || 'unknown'}-${getTimestampValue(message?.messageTimestamp) || Date.now()}`;

  return {
    id: providerMessageId,
    providerMessageId,
    remoteJid,
    participant,
    from,
    fromMe: Boolean(message?.key?.fromMe),
    text: getMessageText(message?.message),
    messageType: getMessageType(message?.message),
    messageTimestamp: getTimestampValue(message?.messageTimestamp),
    messageAt: timestampToIso(message?.messageTimestamp),
    updatedAt: nowIso(),
  };
}

function createSessionRecord({ id, label, workspaceId, connectionId }) {
  return {
    id,
    status: 'pending',
    label: label || null,
    workspaceId: workspaceId || null,
    connectionId: connectionId || null,
    qr: null,
    qrExpiresAt: null,
    connectedAt: null,
    lastActivityAt: null,
    lastError: null,
    socket: null,
    authDir: null,
    isStarting: false,
    phoneNumber: null,
    accountJid: null,
    contacts: new Map(),
    chats: new Map(),
    messages: new Map(),
    historySyncStatus: 'idle',
    historySyncedAt: null,
    historySyncStartedAt: null,
  };
}

function publicSession(session) {
  return {
    id: session.id,
    status: session.status,
    label: session.label || null,
    workspaceId: session.workspaceId || null,
    connectionId: session.connectionId || null,
    phoneNumber: session.phoneNumber || null,
    accountJid: session.accountJid || null,
    lastError: session.lastError || null,
    lastActivityAt: session.lastActivityAt || null,
    connectedAt: session.connectedAt || null,
    qrExpiresAt: session.qrExpiresAt || null,
    hasQr: Boolean(session.qr),
    historySyncStatus: session.historySyncStatus || 'idle',
    historySyncStartedAt: session.historySyncStartedAt || null,
    historySyncedAt: session.historySyncedAt || null,
    contactsCount: session.contacts?.size || 0,
    chatsCount: session.chats?.size || 0,
    messagesCount: session.messages?.size || 0,
  };
}

function upsertContacts(session, contacts = []) {
  for (const contact of contacts) {
    const normalized = normalizeContact(contact);
    if (!normalized.id) continue;
    session.contacts.set(normalized.id, {
      ...(session.contacts.get(normalized.id) || {}),
      ...normalized,
    });
  }
}

function upsertChats(session, chats = []) {
  for (const chat of chats) {
    const normalized = normalizeChat(chat);
    if (!normalized.id) continue;
    session.chats.set(normalized.id, {
      ...(session.chats.get(normalized.id) || {}),
      ...normalized,
    });
  }
}

function trimMessageCache(session) {
  const entries = Array.from(session.messages.values()).sort((a, b) => (a.messageTimestamp || 0) - (b.messageTimestamp || 0));
  while (entries.length > MAX_MESSAGE_CACHE) {
    const oldest = entries.shift();
    if (oldest?.id) {
      session.messages.delete(oldest.id);
    }
  }
}

function upsertMessages(session, messages = []) {
  for (const message of messages) {
    const normalized = normalizeMessage(message);
    if (!normalized.id) continue;

    session.messages.set(normalized.id, {
      ...(session.messages.get(normalized.id) || {}),
      ...normalized,
    });
  }

  trimMessageCache(session);
}

function listSortedValues(map) {
  return Array.from(map.values()).sort((a, b) => {
    const left = a?.displayName || a?.name || a?.jid || a?.id || '';
    const right = b?.displayName || b?.name || b?.jid || b?.id || '';
    return String(left).localeCompare(String(right));
  });
}

async function ensureSessionsDir() {
  await fs.mkdir(getSessionsDir(), { recursive: true });
}

async function ensureSessionDirectory(sessionId) {
  const dir = path.join(getSessionsDir(), sessionId);
  await fs.mkdir(dir, { recursive: true });
  return dir;
}

async function removeSessionDirectory(sessionId) {
  const dir = path.join(getSessionsDir(), sessionId);
  await fs.rm(dir, { recursive: true, force: true });
}

async function emitWebhook(type, payload) {
  try {
    await sendWebhook(type, payload);
  } catch (error) {
    logger.warn({ err: error, type }, 'Webhook delivery failed');
  }
}

async function bindSocket(session, isRestore = false) {
  await ensureSessionsDir();

  const authDir = await ensureSessionDirectory(session.id);
  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  const { version } = await fetchLatestBaileysVersion();

  const socket = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    syncFullHistory: true,
    logger: logger.child({ sessionId: session.id, scope: 'baileys' }),
  });

  session.authDir = authDir;
  session.socket = socket;
  session.isStarting = false;
  session.lastError = null;
  session.historySyncStatus = 'syncing';
  session.historySyncStartedAt = session.historySyncStartedAt || nowIso();

  socket.ev.on('creds.update', saveCreds);

  socket.ev.on('messaging-history.set', async ({ chats, contacts, messages, isLatest }) => {
    upsertContacts(session, contacts || []);
    upsertChats(session, chats || []);
    upsertMessages(session, messages || []);

    session.historySyncStatus = isLatest ? 'ready' : 'syncing';
    session.historySyncedAt = nowIso();
    session.lastActivityAt = nowIso();

    await emitWebhook('session.history_sync', {
      sessionId: session.id,
      workspaceId: session.workspaceId || null,
      connectionId: session.connectionId || null,
      status: session.historySyncStatus,
      contactsCount: session.contacts.size,
      chatsCount: session.chats.size,
      messagesCount: session.messages.size,
      isLatest: Boolean(isLatest),
    });
  });

  socket.ev.on('contacts.upsert', (contacts) => {
    upsertContacts(session, contacts || []);
    session.lastActivityAt = nowIso();
  });

  socket.ev.on('contacts.update', (contacts) => {
    upsertContacts(session, contacts || []);
    session.lastActivityAt = nowIso();
  });

  socket.ev.on('chats.upsert', (chats) => {
    upsertChats(session, chats || []);
    session.lastActivityAt = nowIso();
  });

  socket.ev.on('chats.update', (chats) => {
    upsertChats(session, chats || []);
    session.lastActivityAt = nowIso();
  });

  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      session.qr = qr;
      session.status = 'waiting_for_qr';
      session.qrExpiresAt = new Date(Date.now() + 60_000).toISOString();

      await emitWebhook('session.qr_ready', {
        sessionId: session.id,
        status: session.status,
        qrExpiresAt: session.qrExpiresAt,
        workspaceId: session.workspaceId || null,
        connectionId: session.connectionId || null,
      });
    }

    if (connection === 'connecting' && !qr && session.status === 'waiting_for_qr') {
      session.status = 'pairing';
      await emitWebhook('session.pairing', {
        sessionId: session.id,
        status: session.status,
      });
    }

    if (connection === 'open') {
      session.status = 'connected';
      session.qr = null;
      session.qrExpiresAt = null;
      session.connectedAt = nowIso();
      session.lastActivityAt = nowIso();
      session.accountJid = socket.user?.id || null;
      session.phoneNumber = extractPhoneFromJid(socket.user?.id);

      await emitWebhook('session.connected', {
        sessionId: session.id,
        status: session.status,
        phoneNumber: session.phoneNumber,
        accountJid: session.accountJid,
        workspaceId: session.workspaceId || null,
        connectionId: session.connectionId || null,
        restored: isRestore,
      });
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;

      session.qr = null;
      session.qrExpiresAt = null;
      session.connectedAt = null;
      session.accountJid = null;
      session.phoneNumber = null;
      session.lastError = lastDisconnect?.error?.message || 'Connection closed';
      session.status = loggedOut ? 'needs_reauth' : 'disconnected';

      await emitWebhook(loggedOut ? 'session.reauth_required' : 'session.disconnected', {
        sessionId: session.id,
        status: session.status,
        reason: session.lastError,
      });

      if (!loggedOut) {
        setTimeout(() => {
          startSession({ sessionId: session.id, label: session.label, workspaceId: session.workspaceId, connectionId: session.connectionId, isRestore: true })
            .catch((error) => logger.error({ err: error, sessionId: session.id }, 'Reconnect failed'));
        }, 3_000);
      }
    }
  });

  socket.ev.on('messages.upsert', async ({ messages, type }) => {
    if (!Array.isArray(messages) || messages.length === 0) return;

    upsertMessages(session, messages);
    session.lastActivityAt = nowIso();

    if (type !== 'notify') return;

    for (const msg of messages) {
      if (!msg?.message) continue;
      if (msg.key?.fromMe) continue;

      await emitWebhook('message.received', {
        sessionId: session.id,
        workspaceId: session.workspaceId || null,
        connectionId: session.connectionId || null,
        messageId: msg.key?.id || null,
        remoteJid: msg.key?.remoteJid || null,
        from: jidNormalizedUser(msg.key?.participant || msg.key?.remoteJid || ''),
        text: getMessageText(msg.message),
        messageTimestamp: msg.messageTimestamp || null,
      });
    }
  });
}

export async function startSession({ sessionId, label, workspaceId, connectionId, isRestore = false } = {}) {
  const id = sessionId || uuidv4();
  const existing = sessions.get(id);

  if (existing?.isStarting) {
    return publicSession(existing);
  }

  if (existing?.status === 'connected' || existing?.status === 'waiting_for_qr' || existing?.status === 'pairing') {
    return publicSession(existing);
  }

  const session = existing || createSessionRecord({
    id,
    label,
    workspaceId,
    connectionId,
  });

  session.label = label || session.label;
  session.workspaceId = workspaceId || session.workspaceId;
  session.connectionId = connectionId || session.connectionId;
  session.isStarting = true;
  session.status = 'pending';

  sessions.set(id, session);
  await bindSocket(session, isRestore);
  return publicSession(session);
}

export function getSession(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return null;
  return publicSession(session);
}

export function listSessions() {
  return Array.from(sessions.values()).map(publicSession);
}

export async function getSessionQr(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return null;
  if (!session.qr) {
    return {
      ...publicSession(session),
      qr: null,
      qrDataUrl: null,
    };
  }

  const qrDataUrl = await QRCode.toDataURL(session.qr, { margin: 1, width: 360 });

  return {
    ...publicSession(session),
    qr: session.qr,
    qrDataUrl,
  };
}

export function getSessionContacts(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return null;

  return {
    ...publicSession(session),
    contacts: listSortedValues(session.contacts),
  };
}

export function getSessionChats(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return null;

  const chats = Array.from(session.chats.values()).sort((a, b) => (b.conversationTimestamp || 0) - (a.conversationTimestamp || 0));

  return {
    ...publicSession(session),
    chats,
  };
}

export function getSessionMessages(sessionId, { limit = 100, remoteJid = null } = {}) {
  const session = sessions.get(sessionId);
  if (!session) return null;

  let messages = Array.from(session.messages.values());

  if (remoteJid) {
    messages = messages.filter((message) => message.remoteJid === remoteJid);
  }

  messages.sort((a, b) => (b.messageTimestamp || 0) - (a.messageTimestamp || 0));

  return {
    ...publicSession(session),
    messages: messages.slice(0, Math.max(1, Math.min(Number(limit) || 100, MAX_MESSAGE_CACHE))),
  };
}

export async function sendTextMessage(sessionId, { to, text }) {
  const session = sessions.get(sessionId);
  if (!session?.socket) {
    throw new Error('Session not found');
  }

  if (session.status !== 'connected') {
    throw new Error('Session is not connected');
  }

  const jid = normalizeRecipient(to);
  if (!jid) {
    throw new Error('Recipient is required');
  }

  if (!text) {
    throw new Error('Text is required');
  }

  try {
    const result = await session.socket.sendMessage(jid, { text });
    session.lastActivityAt = nowIso();

    await emitWebhook('message.sent', {
      sessionId: session.id,
      connectionId: session.connectionId || null,
      to: jid,
      text,
      providerMessageId: result?.key?.id || null,
    });

    return {
      ok: true,
      providerMessageId: result?.key?.id || null,
      to: jid,
    };
  } catch (error) {
    await emitWebhook('message.failed', {
      sessionId: session.id,
      connectionId: session.connectionId || null,
      to: jid,
      text,
      error: error.message,
    });
    throw error;
  }
}

export async function deleteSession(sessionId) {
  const session = sessions.get(sessionId);
  if (!session) return false;

  try {
    await session.socket?.logout();
  } catch {
    // ignore logout failures
  }

  sessions.delete(sessionId);
  await removeSessionDirectory(sessionId);

  await emitWebhook('session.disconnected', {
    sessionId,
    status: 'disconnected_by_user',
  });

  return true;
}

export async function restoreSessions() {
  await ensureSessionsDir();
  const entries = await fs.readdir(getSessionsDir(), { withFileTypes: true });

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;

    const id = entry.name;
    if (sessions.has(id)) continue;

    try {
      await startSession({ sessionId: id, isRestore: true });
    } catch (error) {
      logger.error({ err: error, sessionId: id }, 'Failed to restore session');
    }
  }
}
