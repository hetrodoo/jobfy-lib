export type LogStream = 'stdout' | 'stderr';

// 'output' = real job output (counted as an attempt); 'system' = a jobfy
// notice (pickup/stall/failure). Omitted = treated as 'output'.
export type LogEntryKind = 'output' | 'system';

export interface LogEntry {
  stdout: string;
  stderr: string;
  ranBy: string;
  kind?: LogEntryKind;
  // ISO 8601 UTC, set at persist time when omitted.
  timestamp?: string;
}

/**
 * Final-log persistence hook. Receives the aggregated buffers for an attempt.
 * Provide this when the consumer can persist directly (e.g. the jobfy worker
 * writes to Postgres). When omitted, the lib falls back to POSTing to
 * `apiUrl`/`apiKey`; when neither is configured, no final log is persisted
 * (the consumer is expected to handle it inside the handler).
 */
export type PersistLog = (id: string, entry: LogEntry) => Promise<void> | void;

export interface JobfyOptions {
  /**
   * Redis connection URL. Must point to the same Redis the jobfy API/worker
   * fleet uses — that's what carries the queue, cancellation pub/sub, and
   * live-log streams.
   */
  redisUrl: string;

  /**
   * Name of the BullMQ queue this worker consumes. Must be one of the
   * `JOB_QUEUES` configured on the API. Run a dedicated queue for in-process
   * workers — the lib hands every job to your handler regardless of the
   * command prefix (`docker:`, `ssh:`), so mixing it with a queue that's
   * also processed by standard workers risks the wrong worker grabbing a
   * job.
   */
  queueName: string;

  /**
   * HTTP base URL of jobfy-api (no trailing slash). Used to POST the final
   * `{stdout, stderr, ranBy}` log entry after each attempt when no
   * `persistLog` hook is supplied. Optional when `persistLog` is provided
   * (or when the consumer persists logs itself inside the handler).
   */
  apiUrl?: string;

  /**
   * Value of jobfy-api's `API_SECRET_KEY`. Sent as the `x-api-key` header on
   * log-persistence calls. Required only when relying on the HTTP fallback.
   */
  apiKey?: string;

  /**
   * Custom final-log persistence. Takes precedence over the `apiUrl`/`apiKey`
   * HTTP fallback. The in-process jobfy worker uses this to write straight to
   * the database instead of round-tripping over HTTP.
   */
  persistLog?: PersistLog;

  /**
   * Reported as `ranBy` on log entries and shown on the dashboard. Defaults
   * to `os.hostname()`.
   */
  workerName?: string;

  /**
   * BullMQ concurrency. Defaults to 1.
   */
  concurrency?: number;

  /** BullMQ lock duration in ms. Defaults to 120000. */
  lockDuration?: number;

  /** BullMQ stalled-check interval in ms. Defaults to 30000. */
  stalledInterval?: number;

  /**
   * BullMQ max stalled count. Defaults to 1. Values > 1 can re-run a job, so
   * only raise it for idempotent handlers.
   */
  maxStalledCount?: number;

  /**
   * Cap on live-log stream entries (`MAXLEN ~`). Defaults to 250 — matches
   * the API's default. The Redis stream auto-trims as you append.
   */
  maxLogLines?: number;
}

export interface JobContext {
  /** Execution / BullMQ job id (UUID). */
  id: string;
  /** Job name (the execution's jobName). */
  name: string;
  /** Command string from CreateExecutionDto.command. */
  command: string;
  /** Aborted when the user cancels the execution from the dashboard / API. */
  signal: AbortSignal;
  /**
   * Push a chunk of live output for this job. Each call XADDs into the
   * Redis stream the dashboard's live-log viewer reads.
   * Pass complete lines without trailing newlines for cleanest rendering.
   */
  report: (stream: LogStream, text: string) => void;
}

/**
 * Job handler. Whatever it returns becomes the BullMQ job's return value
 * (surfaced as the execution's stored result). Returning nothing is fine.
 */
export type JobHandler = (ctx: JobContext) => unknown;

export type ErrorHandler = (err: Error) => void;

export interface JobfyEvents {
  job: JobHandler;
  error: ErrorHandler;
}
