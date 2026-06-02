// Mirrors jobfy-api/src/redis/channels.ts. Duplicated intentionally so the
// lib stays standalone; if these ever diverge, status updates and
// cancellation silently break. Bump in lock-step.
export const CANCELLATIONS_CHANNEL = 'cancellations';

// Mirrors jobfy-api/src/executions/execution-stream.service.ts.
export const EXEC_STREAM_KEY_PREFIX = 'exec:stream:';
export const EXEC_STREAM_TTL_SECONDS = 3600;
export const DEFAULT_MAX_LOG_LINES = 250;
