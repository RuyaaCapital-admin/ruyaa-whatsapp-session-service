import { Router } from 'express';

import {
  deleteSession,
  getSession,
  getSessionQr,
  listSessions,
  sendTextMessage,
  startSession,
} from '../sessionManager.js';

const router = Router();

router.get('/', (_req, res) => {
  res.json({ sessions: listSessions() });
});

router.post('/', async (req, res, next) => {
  try {
    const session = await startSession({
      sessionId: req.body?.sessionId,
      label: req.body?.label,
      workspaceId: req.body?.workspaceId,
      connectionId: req.body?.connectionId,
    });

    res.status(201).json(session);
  } catch (error) {
    next(error);
  }
});

router.get('/:id', (req, res) => {
  const session = getSession(req.params.id);

  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  res.json(session);
});

router.get('/:id/qr', async (req, res, next) => {
  try {
    const session = await getSessionQr(req.params.id);

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    res.json(session);
  } catch (error) {
    next(error);
  }
});

router.post('/:id/send', async (req, res, next) => {
  try {
    const result = await sendTextMessage(req.params.id, {
      to: req.body?.to,
      text: req.body?.text,
    });

    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.delete('/:id', async (req, res, next) => {
  try {
    const deleted = await deleteSession(req.params.id);

    if (!deleted) {
      return res.status(404).json({ error: 'Session not found' });
    }

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

export default router;
