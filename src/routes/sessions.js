import { Router } from 'express';

import {
  deleteSession,
  getSession,
  getSessionChats,
  getSessionContacts,
  getSessionMessages,
  getSessionQr,
  listSessions,
  resyncSession,
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
      sessionId: req.body?.session_id || req.body?.sessionId || req.body?.connectionId,
      label: req.body?.label,
      workspaceId: req.body?.workspaceId || req.body?.workspace_id,
      connectionId: req.body?.connectionId || req.body?.connection_id,
    });

    res.status(201).json({
      ...session,
      id: session.id,
      session_id: session.id,
      sessionId: session.id,
    });
  } catch (error) {
    next(error);
  }
});

router.get('/:id', (req, res) => {
  const session = getSession(req.params.id);

  if (!session) {
    return res.status(404).json({ error: 'Session not found', code: 'SESSION_NOT_FOUND' });
  }

  res.json(session);
});

router.get('/:id/qr', async (req, res, next) => {
  try {
    const session = await getSessionQr(req.params.id);

    if (!session) {
      return res.status(404).json({ error: 'Session not found', code: 'SESSION_NOT_FOUND' });
    }

    res.json(session);
  } catch (error) {
    next(error);
  }
});

router.get('/:id/contacts', (req, res) => {
  const payload = getSessionContacts(req.params.id);

  if (!payload) {
    return res.status(404).json({ error: 'Session not found', code: 'SESSION_NOT_FOUND' });
  }

  res.json(payload);
});

router.get('/:id/chats', (req, res) => {
  const payload = getSessionChats(req.params.id);

  if (!payload) {
    return res.status(404).json({ error: 'Session not found', code: 'SESSION_NOT_FOUND' });
  }

  res.json(payload);
});

router.get('/:id/messages', (req, res) => {
  const payload = getSessionMessages(req.params.id, {
    limit: req.query?.limit,
    remoteJid: req.query?.remoteJid || null,
  });

  if (!payload) {
    return res.status(404).json({ error: 'Session not found', code: 'SESSION_NOT_FOUND' });
  }

  res.json(payload);
});

router.post('/:id/send', async (req, res, next) => {
  try {
    const result = await sendTextMessage(req.params.id, {
      to: req.body?.to,
      text: req.body?.text,
    });

    res.json(result);
  } catch (error) {
    if (error?.message === 'Session not found') {
      return res.status(404).json({
        success: false,
        code: 'SESSION_NOT_FOUND',
        message: 'WhatsApp session is not loaded. Reconnect required.',
        requested_session_id: req.params.id,
        active_session_ids: listSessions().map(s => s.id),
      });
    }
    if (error?.message === 'Session is not connected') {
      return res.status(400).json({
        success: false,
        code: 'SESSION_NOT_CONNECTED',
        message: 'WhatsApp session is not connected.',
      });
    }
    next(error);
  }
});

router.post('/:id/resync', async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(Number(req.body?.limit || req.query?.limit || 50) || 50, 10), 200);
    const result = await resyncSession(req.params.id, { limit });

    if (!result) {
      return res.status(404).json({
        success: false,
        code: 'SESSION_NOT_FOUND',
        message: 'WhatsApp session is not loaded. Reconnect required.',
        requested_session_id: req.params.id,
        active_session_ids: listSessions().map(s => s.id),
      });
    }

    if (result.success === false && result.code === 'SESSION_NOT_CONNECTED') {
      return res.status(400).json(result);
    }

    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.delete('/:id', async (req, res, next) => {
  try {
    const deleted = await deleteSession(req.params.id);

    if (!deleted) {
      return res.status(404).json({ error: 'Session not found', code: 'SESSION_NOT_FOUND' });
    }

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

export default router;
