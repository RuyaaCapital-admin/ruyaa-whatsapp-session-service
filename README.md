# Ruyaa WhatsApp Session Service

External Node.js microservice for WhatsApp QR pairing and linked-device session handling.

## What it does

- Creates WhatsApp linked-device sessions with QR codes
- Restores sessions from disk after restart
- Sends signed webhook events back to Base44
- Receives outbound send requests from Base44
- Exposes a small REST API for session lifecycle

## Endpoints

- `GET /health`
- `POST /sessions`
- `GET /sessions/:id`
- `GET /sessions/:id/qr`
- `DELETE /sessions/:id`
- `POST /sessions/:id/send`

## Required environment variables

See `.env.example`.

Important values:

- `SERVICE_SECRET` → Base44 sends this in `X-Service-Secret`
- `WEBHOOK_SECRET` → used to HMAC-sign webhook payloads to Base44
- `BASE44_WEBHOOK_URL` → the Base44 `whatsappWebhook` function URL
- `SESSIONS_DIR` → directory for persistent WhatsApp auth/session files

## Local run

```bash
npm install
cp .env.example .env
npm start
```

## Railway deploy

1. Deploy this repo as a Railway service
2. Add a persistent volume mounted to `/data/sessions`
3. Set the environment variables from `.env.example`
4. Add the custom domain `wa-session.ruyaacapital.com`
5. In Base44 set:

```text
WHATSAPP_SESSION_SERVICE_URL=https://wa-session.ruyaacapital.com
```

## Notes

- Keep this service separate from the Base44 repo/runtime
- Use the default Base44 function URL for webhook callbacks if custom-domain routing causes issues
- This service is for a linked-device session model, not the official WhatsApp Cloud API
