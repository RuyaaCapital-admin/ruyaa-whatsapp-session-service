import crypto from 'node:crypto';

function createSignature(body, secret) {
  return crypto.createHmac('sha256', secret).update(body).digest('hex');
}

export async function sendWebhook(type, payload = {}) {
  const url = process.env.BASE44_WEBHOOK_URL;
  const secret = process.env.WEBHOOK_SECRET;

  if (!url || !secret) {
    console.warn('[webhook] BASE44_WEBHOOK_URL or WEBHOOK_SECRET is missing');
    return;
  }

  const body = JSON.stringify({
    type,
    sentAt: new Date().toISOString(),
    ...payload,
  });

  const signature = createSignature(body, secret);

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Webhook-Signature': signature,
      'X-Webhook-Event': type,
    },
    body,
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Webhook failed: ${response.status} ${text}`.trim());
  }
}
