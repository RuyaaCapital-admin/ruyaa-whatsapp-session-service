import express from 'express';

import { requireServiceSecret } from './middleware/auth.js';
import sessionsRouter from './routes/sessions.js';
import { restoreSessions } from './sessionManager.js';

const app = express();
const port = Number(process.env.PORT || 3000);

app.disable('x-powered-by');
app.use(express.json({ limit: '1mb' }));

app.get('/health', (_req, res) => {
  res.json({ ok: true, service: 'ruyaa-whatsapp-session-service' });
});

app.use(requireServiceSecret);
app.use('/sessions', sessionsRouter);

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).json({
    error: error?.message || 'Internal server error',
  });
});

await restoreSessions();

app.listen(port, '0.0.0.0', () => {
  console.log(`WhatsApp session service listening on ${port}`);
});
