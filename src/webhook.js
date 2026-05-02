import crypto from 'node:crypto';

function createSignature(body, secret) {
  return crypto.createHmac('sha256', secret).update(body).digest('hex');
}

function cleanPayload(payload = {}) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return payload || {};
  return { ...payload };
}

const EVENT_TYPE_MAP = {
  'session.history_sync': 'history.complete',
  'message.sent': 'message.status',
  'message.delivered': 'message.status',
  'message.read': 'message.status',
  'message.failed': 'message.status',
  'message.queued': 'message.status',
  'contact.upsert': 'history.contacts',
  'contacts.upsert': 'history.contacts',
  'chat.upsert': 'history.chats',
  'chats.upsert': 'history.chats',
};

function normalizePayloadForEvent(rawType, payload) {
  const p = cleanPayload(payload);

  if (rawType === 'message.sent') {
    return {
      message_id: p.message_id || p.messageId || p.providerMessageId || p.id || null,
      jid: p.jid || p.remoteJid || p.to || null,
      status: 'sent',
      timestamp: p.timestamp || new Date().toISOString(),
      raw: p,
    };
  }

  if (rawType === 'message.delivered' || rawType === 'message.read' || rawType === 'message.failed' || rawType === 'message.queued') {
    return {
      message_id: p.message_id || p.messageId || p.providerMessageId || p.id || null,
      jid: p.jid || p.remoteJid || p.to || null,
      status: rawType.split('.')[1],
      error_message: p.error_message || p.error || null,
      timestamp: p.timestamp || new Date().toISOString(),
      raw: p,
    };
  }

  return p;
}

export async function sendWebhook(type, payload = {}) {
  const url = process.env.WEBHOOK_URL || process.env.BASE44_WEBHOOK_URL;
  const secret = process.env.WEBHOOK_SECRET;

  if (!url || !secret) {
    console.warn('[webhook] WEBHOOK_URL/BASE44_WEBHOOK_URL or WEBHOOK_SECRET is missing');
    return;
  }

  const normalizedEventType = EVENT_TYPE_MAP[type] || type;
  if (!normalizedEventType) {
    throw new Error('sendWebhook missing event_type');
  }

  const sessionId =
    payload?.session_id ||
    payload?.sessionId ||
    payload?.id ||
    payload?.connectionId ||
    null;

  if (!sessionId) {
    throw new Error(`sendWebhook missing session_id for ${type}`);
  }

  const normalizedPayload = normalizePayloadForEvent(type, payload);
  const idempotencyKey = `${sessionId}:${normalizedEventType}:${Date.now()}:${Math.random().toString(36).slice(2)}`;

  const bodyObject = {
    event_type: normalizedEventType,
    raw_event_type: type,
    session_id: sessionId,
    payload: normalizedPayload,
    idempotency_key: idempotencyKey,
    emitted_at: new Date().toISOString(),
  };

  const body = JSON.stringify(bodyObject);
  const signature = createSignature(body, secret);

  const sentAt = Date.now();
  console.log('[webhook] sending', JSON.stringify({
    event_type: bodyObject.event_type,
    raw_event_type: bodyObject.raw_event_type,
    session_id: bodyObject.session_id,
    payload_keys: normalizedPayload && typeof normalizedPayload === 'object' ? Object.keys(normalizedPayload).slice(0, 20) : [],
  }));

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Webhook-Signature': signature,
      'X-Webhook-Event': normalizedEventType,
      'X-Webhook-Raw-Event': type,
    },
    body,
  });

  console.log('[webhook] response', JSON.stringify({
    event_type: bodyObject.event_type,
    raw_event_type: bodyObject.raw_event_type,
    session_id: bodyObject.session_id,
    status: response.status,
    ok: response.ok,
    duration_ms: Date.now() - sentAt,
  }));

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Webhook failed: ${response.status} ${text}`.trim());
  }
}
