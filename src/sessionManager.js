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
  };
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
    syncFullHistory: false,
    logger: logger.child({ sessionId: session.id, scope: 'baileys' }),
  });

  session.authDir = authDir;
  session.socket = socket;
  session.isStarting = false;
  session.lastError = null;

  socket.ev.on('creds.update', saveCreds);

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
    if (type !== 'notify') return;

    for (const msg of messages || []) {
      if (!msg?.message) continue;
      if (msg.key?.fromMe) continue;

      session.lastActivityAt = nowIso();

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

  const session = existing || {
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
    isStarting: false,
    phoneNumber: null,
    accountJid: null,
  };

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
