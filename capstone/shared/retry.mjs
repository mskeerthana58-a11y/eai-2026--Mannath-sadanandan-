/** Failed attempts so far, from the x-death header RabbitMQ adds at each dead-lettering (PA5's getRetryCount). */
export function getRetryCount(msg) {
  const xDeath = msg.properties.headers?.['x-death'];
  if (!Array.isArray(xDeath)) return 0;
  return xDeath.filter((e) => e?.reason === 'rejected').reduce((sum, e) => sum + (Number(e?.count) || 0), 0);
}

/** Headers without the retry history, so a replayed message gets fresh attempts. */
export function withoutRetryHistory(headers) {
  const kept = {};
  for (const [key, value] of Object.entries(headers ?? {})) {
    if (key === 'x-death' || key.startsWith('x-first-death-') || key.startsWith('x-last-death-')) continue;
    kept[key] = value;
  }
  return kept;
}
