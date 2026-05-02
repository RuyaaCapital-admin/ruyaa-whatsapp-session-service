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
const HISTORY_CHAT_LIMIT = Number(process.env.INITIAL_HISTORY_CHAT_LIMIT || 50);
const HISTORY_SNAPSHOT_COOLDOWN_MS = Number(process.env.HISTORY_SNAPSHOT_COOLDOWN_MS || 30_000);
const HISTORY_SNAPSHOT_DELAY_MS = Number(process.env.HISTORY_SNAPSHOT_DELAY_MS || 8_000);
const AVATAR_HYDRATE_LIMIT = Number(process.env.AVATAR_HYDRATE_LIMIT || 12);

function nowIso() {
  return new Date().toISOString();
}

function logEvent(step, fields = {}) {
  // Plain JSON log line keyed by the inbound pipeline step. Pairs with the
  // matching log lines in base44/whatsappWebhook so a payload can be traced
  // across both services.
  try {
    console.log(JSON.stringify({ service: 'whatsapp-session', step, ts: nowIso(), ...fields }));
  } catch {
    /* ignore log serialization errors */
  }
}

function getSessionsDir() {
  return process.env.SESSIONS_DIR || path.resolve('data/sessions');
}

function extractPhoneFromJid(jid) {
  if (!jid) return null;
  return String(jid).split(':')[0].split('@')[0] || null;
}

function normalizePhone(value) {
  return value ? String(value).replace(/\D/g, '') : '';
}

function isValidPhoneNumber(phone) {
  const digits = normalizePhone(phone);
  return digits.length >= 7 && digits.length <= 16 && digits !== '0';
}

function isStatusOrBroadcastJid(jid) {
  const value = String(jid || '');
  return value.includes('status@') || value.includes('@broadcast') || value.includes('newsletter') || value === '0';
}

function isGroupJid(jid) {
  return String(jid || '').includes('@g.us');
}

function isDirectWhatsappJid(jid) {
  const value = String(jid || '');
  if (!value || isStatusOrBroadcastJid(value) || isGroupJid(value)) return false;
  if (!value.includes('@s.whatsapp.net') && !value.includes('@c.us')) return false;
  return isValidPhoneNumber(extractPhoneFromJid(value));
}

function isImportableChatJid(jid) {
  // Keep the first version strict: direct user chats only. Groups/status/broadcasts
  // are noisy and were creating garbage rows in Base44.
  return isDirectWhatsappJid(jid);
}

function normalizeRecipient(to) {
  if (!to) return null;
  if (String(to).includes('@')) return to;
  const cleaned = normalizePhone(to);
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
  const first = keys[0] || 'unknown';
  if (first === 'conversation' || first === 'extendedTextMessage') return 'text';
  if (first.endsWith('Message')) return first.replace('Message', '');
  return first;
}

function isMediaType(type) {
  return ['image', 'video', 'document', 'audio', 'sticker'].includes(type);
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
  if (!isDirectWhatsappJid(jid)) return null;

  const phoneNumber = extractPhoneFromJid(jid);
  if (!isValidPhoneNumber(phoneNumber)) return null;

  const realName = contact.name || contact.verifiedName || contact.notify || contact.pushName || contact.short || null;
  const displayName = realName || phoneNumber;
  const avatar = contact.imgUrl || contact.avatarUrl || contact.profilePicUrl || contact.avatar_url || null;

  return {
    id: jid,
    jid,
    phone: phoneNumber,
    phoneNumber,
    phone_number: phoneNumber,
    phone_number_normalized: phoneNumber,
    name: realName || displayName,
    displayName,
    display_name: displayName,
    pushName: contact.notify || contact.pushName || null,
    push_name: contact.notify || contact.pushName || null,
    verifiedName: contact.verifiedName || null,
    verified_name: contact.verifiedName || null,
    shortName: contact.short || null,
    short_name: contact.short || null,
    avatarUrl: avatar,
    avatar_url: avatar,
    avatar_last_fetched_at: avatar ? nowIso() : null,
    isBusiness: Boolean(contact.verifiedName),
    is_business: Boolean(contact.verifiedName),
    updatedAt: nowIso(),
  };
}

function normalizeChat(chat = {}) {
  const jid = chat.id || null;
  if (!isImportableChatJid(jid)) return null;

  const conversationTimestamp = chat.conversationTimestamp || chat.lastMessageRecvTimestamp || null;
  const phone = extractPhoneFromJid(jid);
  const name = chat.name || chat.subject || chat.title || phone;

  return {
    id: jid,
    jid,
    phone,
    name,
    title: name,
    subject: chat.subject || null,
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
  if (!isImportableChatJid(remoteJid)) return null;

  const participant = message?.key?.participant || null;
  const from = jidNormalizedUser(participant || remoteJid || '');
  const providerMessageId = message?.key?.id || `${remoteJid || 'unknown'}-${getTimestampValue(message?.messageTimestamp) || Date.now()}`;
  const type = getMessageType(message?.message);
  const text = getMessageText(message?.message);
  const displayText = text || (isMediaType(type) ? `[${type}]` : '');
  if (!displayText) return null;

  const timestamp = timestampToIso(message?.messageTimestamp) || nowIso();

  return {
    id: providerMessageId,
    messageId: providerMessageId,
    message_id: providerMessageId,
    providerMessageId,
    provider_message_id: providerMessageId,
    jid: remoteJid,
    remoteJid,
    participant,
    from,
    fromMe: Boolean(message?.key?.fromMe),
    fromSelf: Boolean(message?.key?.fromMe),
    text: displayText,
    body: displayText,
    type,
    messageType: type,
    message_type: type,
    messageTimestamp: getTimestampValue(message?.messageTimestamp) || Date.now(),
    timestamp,
    messageAt: timestamp,
    pushName: message?.pushName || null,
    senderName: message?.pushName || null,
    sender_name: message?.pushName || null,
    updatedAt: nowIso(),
  };
}

function mapAckToStatus(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    if (numeric <= 0) return 'failed';
    if (numeric === 1 || numeric === 2) return 'sent';
    if (numeric === 3) return 'delivered';
    if (numeric >= 4) return 'read';
  }

  const text = String(value || '').toLowerCase();
  if (text.includes('read') || text.includes('played')) return 'read';
  if (text.includes('deliver')) return 'delivered';
  if (text.includes('server') || text.includes('sent') || text.includes('ack')) return 'sent';
  if (text.includes('error') || text.includes('fail')) return 'failed';
  return null;
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
    avatarCache: new Map(),
    historySnapshotTimer: null,
    lastHistorySnapshotSentAt: 0,
    historySyncStatus: 'idle',
    historySyncedAt: null,
    historySyncStartedAt: null,
  };
}

function publicSession(session) {
  return {
    id: session.id,
    session_id: session.id,
    sessionId: session.id,
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
    if (!normalized?.id) continue;
    session.contacts.set(normalized.id, {
      ...(session.contacts.get(normalized.id) || {}),
      ...normalized,
    });
  }
}

function ensureContactForChat(session, chat) {
  if (!chat?.jid || !isDirectWhatsappJid(chat.jid)) return null;
  const existing = session.contacts.get(chat.jid);
  if (existing) return existing;

  const phone = extractPhoneFromJid(chat.jid);
  if (!isValidPhoneNumber(phone)) return null;

  const contact = {
    id: chat.jid,
    jid: chat.jid,
    phone,
    phoneNumber: phone,
    phone_number: phone,
    phone_number_normalized: phone,
    name: chat.name || phone,
    displayName: chat.name || phone,
    display_name: chat.name || phone,
    pushName: null,
    push_name: null,
    avatarUrl: null,
    avatar_url: null,
    updatedAt: nowIso(),
  };
  session.contacts.set(chat.jid, contact);
  return contact;
}

function upsertChats(session, chats = []) {
  for (const chat of chats) {
    const normalized = normalizeChat(chat);
    if (!normalized?.id) continue;
    session.chats.set(normalized.id, {
      ...(session.chats.get(normalized.id) || {}),
      ...normalized,
    });
    ensureContactForChat(session, normalized);
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
    if (!normalized?.id) continue;

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

function listRecentChats(session, limit = 50) {
  return Array.from(session.chats.values())
    .filter((c) => isImportableChatJid(c?.jid || c?.id))
    .sort((a, b) => (b.conversationTimestamp || 0) - (a.conversationTimestamp || 0))
    .slice(0, Math.max(1, Math.min(Number(limit) || 50, 200)));
}

function listRecentMessages(session, limit = 500, chatIds = null) {
  let messages = Array.from(session.messages.values()).filter((m) => m?.text && isImportableChatJid(m.remoteJid || m.jid));
  if (chatIds?.size) {
    messages = messages.filter((m) => chatIds.has(m.remoteJid) || chatIds.has(m.jid));
  }
  return messages
    .sort((a, b) => (b.messageTimestamp || 0) - (a.messageTimestamp || 0))
    .slice(0, Math.max(1, Math.min(Number(limit) || 500, MAX_MESSAGE_CACHE)));
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

async function fetchAvatar(session, jid) {
  if (!jid || !session.socket?.profilePictureUrl || !isDirectWhatsappJid(jid)) return null;
  const cached = session.avatarCache.get(jid);
  const now = Date.now();
  if (cached && now - cached.fetchedAt < 24 * 60 * 60 * 1000) return cached.url || null;

  try {
    const url = await Promise.race([
      session.socket.profilePictureUrl(jid, 'image'),
      new Promise((_, reject) => setTimeout(() => reject(new Error('avatar_timeout')), 3500)),
    ]);
    session.avatarCache.set(jid, { url: url || null, fetchedAt: now });
    return url || null;
  } catch {
    session.avatarCache.set(jid, { url: null, fetchedAt: now });
    return null;
  }
}

async function hydrateRecentAvatars(session, chats = []) {
  const targets = chats.slice(0, AVATAR_HYDRATE_LIMIT).filter((chat) => isDirectWhatsappJid(chat.jid));
  for (const chat of targets) {
    const contact = ensureContactForChat(session, chat);
    if (!contact || contact.avatar_url || contact.avatarUrl) continue;
    const avatar = await fetchAvatar(session, chat.jid);
    if (!avatar) continue;
    session.contacts.set(chat.jid, {
      ...contact,
      avatarUrl: avatar,
      avatar_url: avatar,
      avatar_last_fetched_at: nowIso(),
      updatedAt: nowIso(),
    });
  }
}

async function emitHistorySnapshot(session, { limit = HISTORY_CHAT_LIMIT, isLatest = false, source = 'snapshot' } = {}) {
  const chats = listRecentChats(session, limit);
  await hydrateRecentAvatars(session, chats);

  const chatIds = new Set(chats.map((c) => c.jid || c.id).filter(Boolean));
  const messages = listRecentMessages(session, Math.min(limit * 10, 500), chatIds);
  const contacts = chats
    .map((chat) => ensureContactForChat(session, chat))
    .filter(Boolean)
    .filter((contact) => chatIds.has(contact.jid || contact.id));

  const basePayload = {
    sessionId: session.id,
    workspaceId: session.workspaceId || null,
    connectionId: session.connectionId || null,
    source,
  };

  logger.info({ sessionId: session.id, contacts: contacts.length, chats: chats.length, messages: messages.length, source }, 'Emitting parsed WhatsApp history snapshot');

  if (contacts.length) {
    await emitWebhook('history.contacts', { ...basePayload, items: contacts });
  }
  if (chats.length) {
    await emitWebhook('history.chats', { ...basePayload, items: chats });
  }
  if (messages.length) {
    await emitWebhook('history.messages', { ...basePayload, items: messages });
  }

  await emitWebhook('history.complete', {
    ...basePayload,
    contactsCount: contacts.length,
    chatsCount: chats.length,
    messagesCount: messages.length,
    total_contacts: contacts.length,
    total_chats: chats.length,
    total_messages: messages.length,
    isLatest: Boolean(isLatest),
  });

  session.lastHistorySnapshotSentAt = Date.now();

  return {
    contacts_imported: contacts.length,
    chats_imported: chats.length,
    messages_imported: messages.length,
    status: contacts.length || chats.length || messages.length ? 'ready' : 'empty_result',
  };
}

function scheduleHistorySnapshot(session, { force = false, isLatest = false, source = 'scheduled' } = {}) {
  if (!session) return;
  if (session.historySnapshotTimer) clearTimeout(session.historySnapshotTimer);

  const elapsed = Date.now() - (session.lastHistorySnapshotSentAt || 0);
  if (!force && !isLatest && elapsed < HISTORY_SNAPSHOT_COOLDOWN_MS) {
    return;
  }

  const delay = force || isLatest ? 1200 : HISTORY_SNAPSHOT_DELAY_MS;
  session.historySnapshotTimer = setTimeout(() => {
    session.historySnapshotTimer = null;
    emitHistorySnapshot(session, { limit: HISTORY_CHAT_LIMIT, isLatest, source })
      .catch((error) => logger.warn({ err: error, sessionId: session.id }, 'History snapshot emit failed'));
  }, delay);
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

  socket.ev.on('messaging-history.set', ({ chats, contacts, messages, isLatest }) => {
    upsertContacts(session, contacts || []);
    upsertChats(session, chats || []);
    upsertMessages(session, messages || []);

    session.historySyncStatus = isLatest ? 'ready' : 'syncing';
    session.historySyncedAt = nowIso();
    session.lastActivityAt = nowIso();

    // Important: Baileys sends history in many chunks. Do NOT blast Base44 on every
    // chunk; that is what was causing 429s and half-imports. Emit once after the
    // latest chunk, or a cooled-down partial snapshot if latest never arrives.
    scheduleHistorySnapshot(session, {
      force: Boolean(isLatest),
      isLatest: Boolean(isLatest),
      source: 'messaging-history.set',
    });
  });

  socket.ev.on('contacts.upsert', (contacts) => {
    upsertContacts(session, contacts || []);
    session.lastActivityAt = nowIso();
    scheduleHistorySnapshot(session, { source: 'contacts.upsert' });
  });

  socket.ev.on('contacts.update', (contacts) => {
    upsertContacts(session, contacts || []);
    session.lastActivityAt = nowIso();
    scheduleHistorySnapshot(session, { source: 'contacts.update' });
  });

  socket.ev.on('chats.upsert', (chats) => {
    upsertChats(session, chats || []);
    session.lastActivityAt = nowIso();
    scheduleHistorySnapshot(session, { source: 'chats.upsert' });
  });

  socket.ev.on('chats.update', (chats) => {
    upsertChats(session, chats || []);
    session.lastActivityAt = nowIso();
    scheduleHistorySnapshot(session, { source: 'chats.update' });
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

    const normalizedMessages = messages.map(normalizeMessage).filter(Boolean);
    logEvent('messages_upsert', {
      session_id: session.id,
      connection_id: session.connectionId || null,
      baileys_type: type,
      raw_count: messages.length,
      normalized_count: normalizedMessages.length,
    });
    if (!normalizedMessages.length) return;

    // Live notify events should show immediately in Base44. Phone-side outbound
    // messages are real history.messages rows, not status-only events.
    if (type === 'notify') {
      for (const normalized of normalizedMessages) {
        // Direction is decided ONLY from Baileys' key.fromMe bit which we
        // captured into normalized.fromMe / fromSelf. We never look at the
        // pushName / display name for direction.
        if (normalized.fromSelf) {
          logEvent('emit_outbound_from_phone', {
            session_id: session.id,
            connection_id: session.connectionId || null,
            jid: normalized.remoteJid,
            message_id: normalized.id,
            from_me: true,
          });
          await emitWebhook('history.messages', {
            sessionId: session.id,
            workspaceId: session.workspaceId || null,
            connectionId: session.connectionId || null,
            source: 'messages.upsert.from_me',
            items: [normalized],
          });
          continue;
        }

        const avatar = await fetchAvatar(session, normalized.remoteJid);
        const contact = ensureContactForChat(session, { jid: normalized.remoteJid, name: normalized.pushName || extractPhoneFromJid(normalized.remoteJid) });
        if (contact && (normalized.pushName || avatar)) {
          session.contacts.set(normalized.remoteJid, {
            ...contact,
            name: normalized.pushName || contact.name,
            displayName: normalized.pushName || contact.displayName,
            display_name: normalized.pushName || contact.display_name,
            pushName: normalized.pushName || contact.pushName,
            push_name: normalized.pushName || contact.push_name,
            avatarUrl: avatar || contact.avatarUrl,
            avatar_url: avatar || contact.avatar_url,
            avatar_last_fetched_at: avatar ? nowIso() : contact.avatar_last_fetched_at,
            updatedAt: nowIso(),
          });

          await emitWebhook('history.contacts', {
            sessionId: session.id,
            workspaceId: session.workspaceId || null,
            connectionId: session.connectionId || null,
            source: 'live-message-contact-hydration',
            items: [session.contacts.get(normalized.remoteJid)],
          });
        }

        logEvent('emit_inbound_message_received', {
          session_id: session.id,
          connection_id: session.connectionId || null,
          jid: normalized.remoteJid,
          message_id: normalized.id,
          from_me: false,
          push_name: normalized.pushName || null,
          message_type: normalized.type,
        });
        await emitWebhook('message.received', {
          sessionId: session.id,
          workspaceId: session.workspaceId || null,
          connectionId: session.connectionId || null,
          message_id: normalized.id,
          messageId: normalized.id,
          jid: normalized.remoteJid,
          remoteJid: normalized.remoteJid,
          from_number: extractPhoneFromJid(normalized.remoteJid),
          from_name: normalized.pushName || null,
          push_name: normalized.pushName || null,
          pushName: normalized.pushName || null,
          avatar_url: avatar || null,
          message_content: normalized.text,
          text: normalized.text,
          message_type: normalized.type,
          timestamp: normalized.timestamp,
          messageTimestamp: normalized.messageTimestamp,
          from_self: false,
          fromSelf: false,
          fromMe: false,
          is_group: false,
          is_status: false,
          is_broadcast: false,
        });
      }
    } else {
      logEvent('messages_upsert_skipped_non_notify', {
        session_id: session.id,
        connection_id: session.connectionId || null,
        baileys_type: type,
      });
    }
  });

  socket.ev.on('messages.update', async (updates = []) => {
    for (const update of updates) {
      const status = mapAckToStatus(update?.update?.status ?? update?.update?.ack ?? update?.status ?? update?.ack);
      const messageId = update?.key?.id;
      const jid = update?.key?.remoteJid;
      if (!status || !messageId || !isImportableChatJid(jid)) continue;

      await emitWebhook('message.status', {
        sessionId: session.id,
        workspaceId: session.workspaceId || null,
        connectionId: session.connectionId || null,
        message_id: messageId,
        jid,
        status,
        raw_ack: update?.update?.status ?? update?.update?.ack ?? update?.status ?? update?.ack,
        timestamp: nowIso(),
      });
    }
  });

  socket.ev.on('presence.update', async ({ id, presences }) => {
    if (!isImportableChatJid(id)) return;
    const entries = Object.values(presences || {});
    const state = entries.find(Boolean)?.lastKnownPresence || null;
    if (!state) return;
    const presence = state === 'composing' ? 'composing' : state === 'paused' ? 'paused' : state;
    await emitWebhook('presence.update', {
      sessionId: session.id,
      workspaceId: session.workspaceId || null,
      connectionId: session.connectionId || null,
      jid: id,
      presence,
      timestamp: nowIso(),
    });
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

  const chats = listRecentChats(session, 200);

  return {
    ...publicSession(session),
    chats,
  };
}

export function getSessionMessages(sessionId, { limit = 100, remoteJid = null } = {}) {
  const session = sessions.get(sessionId);
  if (!session) return null;

  let messages = Array.from(session.messages.values()).filter((m) => m?.text && isImportableChatJid(m.remoteJid || m.jid));

  if (remoteJid) {
    messages = messages.filter((message) => message.remoteJid === remoteJid || message.jid === remoteJid);
  }

  messages.sort((a, b) => (b.messageTimestamp || 0) - (a.messageTimestamp || 0));

  return {
    ...publicSession(session),
    messages: messages.slice(0, Math.max(1, Math.min(Number(limit) || 100, MAX_MESSAGE_CACHE))),
  };
}

export async function resyncSession(sessionId, { limit = 50 } = {}) {
  const session = sessions.get(sessionId);
  if (!session) return null;
  if (session.status !== 'connected') {
    return {
      success: false,
      code: 'SESSION_NOT_CONNECTED',
      message: 'WhatsApp session is not connected.',
      status: session.status,
    };
  }

  const result = await emitHistorySnapshot(session, {
    limit,
    isLatest: true,
    source: 'manual-resync',
  });

  return {
    success: true,
    limit,
    ...result,
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

    await emitWebhook('message.status', {
      sessionId: session.id,
      connectionId: session.connectionId || null,
      jid,
      status: 'sent',
      text,
      providerMessageId: result?.key?.id || null,
      message_id: result?.key?.id || null,
      timestamp: nowIso(),
    });

    return {
      ok: true,
      providerMessageId: result?.key?.id || null,
      to: jid,
    };
  } catch (error) {
    await emitWebhook('message.status', {
      sessionId: session.id,
      connectionId: session.connectionId || null,
      jid,
      status: 'failed',
      text,
      error_message: error.message,
      timestamp: nowIso(),
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

  if (session.historySnapshotTimer) clearTimeout(session.historySnapshotTimer);
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
