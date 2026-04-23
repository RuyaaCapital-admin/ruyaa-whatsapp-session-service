import crypto from 'node:crypto';

function timingSafeEqualString(a, b) {
  const left = Buffer.from(String(a ?? ''));
  const right = Buffer.from(String(b ?? ''));

  if (left.length !== right.length) {
    return false;
  }

  return crypto.timingSafeEqual(left, right);
}

export function requireServiceSecret(req, res, next) {
  const expected = process.env.SERVICE_SECRET;

  if (!expected) {
    return res.status(500).json({ error: 'SERVICE_SECRET is not configured' });
  }

  const provided = req.header('X-Service-Secret');

  if (!timingSafeEqualString(provided, expected)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  next();
}
