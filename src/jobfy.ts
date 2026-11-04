import * as os from 'node:os';
import { Job, Worker } from 'bullmq';
import { Redis } from 'ioredis';
import {
  CANCELLATIONS_CHANNEL,
  DEFAULT_MAX_LOG_LINES,
  EXEC_STREAM_KEY_PREFIX,
  EXEC_STREAM_TTL_SECONDS,
} from './constants';
import { LogBuffer } from './log-buffer';
import type {
  ErrorHandler,
  JobContext,
  JobHandler,
  JobfyOptions,
  LogEntry,
  LogStream,
  PersistLog,
  RunningJobInfo,
  WarnHandler,
} from './types';

const LOCK_RENEW_ERROR_PREFIX = 'could not renew lock for job ';
const LAST_REPORT_MAX_CHARS = 300;

interface JobData {
  command: string;
}

interface RunningJob {
  controller: AbortController;
  attempt: number;
  name: string;
  startedAt: number;
  // Last ctx.report() — when a job hangs, this is the best hint of where.
  lastReportAt: number | null;
  lastReportText: string | null;
  // In-flight live-log stream writes. Awaited before the job finishes so a
  // job never reaches a terminal state with the tail of its output still
  // un-flushed to the stream.
  pendingWrites: Promise<void>[];
}

export class Jobfy {
  private readonly redisUrl: string;
  private readonly queueName: string;
  private readonly apiUrl: string | null;
  private readonly apiKey: string | null;
  private readonly persistLog: PersistLog | null;
  private readonly workerName: string;
  private readonly concurrency: number;
  private readonly lockDuration: number;
  private readonly stalledInterval: number;
  private readonly maxStalledCount: number;
  private readonly maxLogLines: number;

  private jobHandler: JobHandler | null = null;
  private errorHandler: ErrorHandler | null = null;
  private warnHandler: WarnHandler | null = null;

  // Three Redis clients on purpose:
  //   - commandClient: XADD log lines, POST not needed (HTTP)
  //   - subscriberClient: SUBSCRIBE for cancellations (ioredis rejects regular
  //     commands once SUBSCRIBE is issued, hence the separate connection)
  //   - bullmq Worker: bullmq owns its own connection internally
  private commandClient: Redis | null = null;
  private subscriberClient: Redis | null = null;
  private worker: Worker<JobData> | null = null;

  private readonly buffers = new LogBuffer();
  private readonly running = new Map<string, RunningJob>();

  constructor(opts: JobfyOptions) {
    if (!opts.redisUrl) throw new Error('Jobfy: redisUrl is required');
    if (!opts.queueName) throw new Error('Jobfy: queueName is required');

    this.redisUrl = opts.redisUrl;
    this.queueName = opts.queueName;
    this.apiUrl = opts.apiUrl ? opts.apiUrl.replace(/\/+$/, '') : null;
    this.apiKey = opts.apiKey ?? null;
    this.persistLog = opts.persistLog ?? null;
    this.workerName = opts.workerName ?? os.hostname();
    this.concurrency = opts.concurrency ?? 1;
    // Lock/stall defaults match jobfy-api's worker.
    this.lockDuration = opts.lockDuration ?? 120_000;
    this.stalledInterval = opts.stalledInterval ?? 30_000;
    this.maxStalledCount = opts.maxStalledCount ?? 1;
    this.maxLogLines = opts.maxLogLines ?? DEFAULT_MAX_LOG_LINES;
    // When neither persistLog nor apiUrl/apiKey is set, the lib skips final-log
    // persistence and the consumer is expected to persist inside its handler.
    // Live-log streaming and cancellation work regardless.
  }

  /**
   * Register a handler. `on('job', fn)` is the one you care about; `on('error', fn)`
   * receives infra-side errors (Worker errors, log-persist failures) so they
   * don't go silently to stderr.
   */
  on(event: 'job', handler: JobHandler): this;
  on(event: 'error', handler: ErrorHandler): this;
  on(event: 'warn', handler: WarnHandler): this;
  on(
    event: 'job' | 'error' | 'warn',
    handler: JobHandler | ErrorHandler | WarnHandler,
  ): this {
    if (event === 'job') {
      this.jobHandler = handler as JobHandler;
    } else if (event === 'error') {
      this.errorHandler = handler as ErrorHandler;
    } else {
      this.warnHandler = handler as WarnHandler;
    }
    return this;
  }

  /**
   * Snapshot of the jobs this process is running right now — for watchdogs
   * that flag jobs running far longer than expected.
   */
  getRunningJobs(): RunningJobInfo[] {
    const now = Date.now();
    return [...this.running.entries()].map(([id, job]) =>
      this.describeRunning(id, job, now),
    );
  }

  /**
   * Public escape hatch for code that doesn't have the JobContext at hand
   * (deeply nested helpers, EventEmitter listeners, etc.). Inside a handler
   * prefer `ctx.report(stream, text)`.
   */
  report(stream: LogStream, id: string, text: string): void {
    if (!this.commandClient) {
      throw new Error('Jobfy: report() called before listen()');
    }
    const running = this.running.get(id);
    const attempt = running?.attempt ?? 1;
    const write = this.appendToStream(id, attempt, stream, text);
    if (running) {
      running.lastReportAt = Date.now();
      running.lastReportText = text.slice(0, LAST_REPORT_MAX_CHARS);
      // Only accumulate for final persistence while the job is live — a buffer
      // for a non-running id would never be drained (memory leak) and couldn't
      // be persisted anyway.
      this.buffers.append(id, stream, text);
      running.pendingWrites.push(write);
    } else {
      void write;
    }
  }

  /**
   * Open Redis connections, start the BullMQ Worker, subscribe to
   * cancellations. Resolves once the Worker is ready to receive jobs.
   */
  async listen(): Promise<void> {
    if (!this.jobHandler) {
      throw new Error(
        'Jobfy: register a handler via on("job", ...) before calling listen()',
      );
    }
    if (this.worker) {
      throw new Error('Jobfy: listen() already called');
    }

    this.commandClient = new Redis(this.redisUrl, {
      maxRetriesPerRequest: 3,
    });
    // maxRetriesPerRequest must be null on the subscriber connection — ioredis
    // re-subscribes on reconnect, and a finite retry budget breaks that,
    // silently dropping cancellations.
    this.subscriberClient = new Redis(this.redisUrl, {
      maxRetriesPerRequest: null,
    });

    await this.subscribeToCancellations();

    // BullMQ's connection options are ioredis RedisOptions, which take
    // host/port/username/password — NOT a `url` field. Parse the URL the same
    // way the jobfy API does so a remote Redis actually gets used.
    const url = new URL(this.redisUrl);
    this.worker = new Worker<JobData>(
      this.queueName,
      (job) => this.processJob(job),
      {
        connection: {
          host: url.hostname,
          port: url.port ? Number(url.port) : 6379,
          username: url.username || undefined,
          password: url.password || undefined,
        },
        concurrency: this.concurrency,
        lockDuration: this.lockDuration,
        stalledInterval: this.stalledInterval,
        maxStalledCount: this.maxStalledCount,
      },
    );

    this.worker.on('error', (err) => {
      // BullMQ emits a bare "could not renew lock for job X" right after
      // 'lockRenewalFailed'; that one is reported below with the job context.
      if (err.message.startsWith(LOCK_RENEW_ERROR_PREFIX)) return;
      this.emitError(err);
    });

    // The lock for these jobs is gone from Redis (cancelled/removed from the
    // dashboard, stalled and re-queued, or Redis lost the key). BullMQ can no
    // longer complete them, so the handler is only holding a concurrency slot:
    // log where it is and abort it so a signal-aware handler frees the slot.
    this.worker.on('lockRenewalFailed', (jobIds: string[]) => {
      const now = Date.now();
      for (const jobId of jobIds) {
        const running = this.running.get(jobId);
        if (!running) {
          this.emitError(
            new Error(
              `${LOCK_RENEW_ERROR_PREFIX}${jobId} (not running in this process)`,
            ),
          );
          continue;
        }
        const info = this.describeRunning(jobId, running, now);
        this.emitError(
          new Error(
            `${LOCK_RENEW_ERROR_PREFIX}${jobId} — ${this.formatRunning(info)}; aborting handler`,
          ),
        );
        running.controller.abort(new Error('jobfy: job lock lost'));
      }
    });

    this.worker.on('stalled', (jobId: string, prev: string) => {
      const running = this.running.get(jobId);
      const where = running
        ? this.formatRunning(this.describeRunning(jobId, running, Date.now()))
        : 'not running in this process';
      this.emitWarn(
        `job ${jobId} stalled (was ${prev}) and was moved back to wait — ${where}`,
      );
    });

    await this.worker.waitUntilReady();
  }

  /**
   * Graceful shutdown — drains the Worker, closes Redis. Call from your
   * SIGTERM / SIGINT handlers.
   */
  async close(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = null;
    }
    if (this.subscriberClient) {
      await this.subscriberClient.quit().catch(() => undefined);
      this.subscriberClient = null;
    }
    if (this.commandClient) {
      await this.commandClient.quit().catch(() => undefined);
      this.commandClient = null;
    }
  }

  private async processJob(job: Job<JobData>): Promise<unknown> {
    if (!job.id) throw new Error('Jobfy: received BullMQ job without an id');
    if (!this.jobHandler) {
      throw new Error('Jobfy: no handler registered');
    }

    const id = job.id;
    const controller = new AbortController();
    const attempt = job.attemptsMade + 1;
    const runningJob: RunningJob = {
      controller,
      attempt,
      name: job.name,
      startedAt: Date.now(),
      lastReportAt: null,
      lastReportText: null,
      pendingWrites: [],
    };
    this.running.set(id, runningJob);

    const ctx: JobContext = {
      id,
      name: job.name,
      command: job.data.command,
      signal: controller.signal,
      report: (stream, text) => this.report(stream, id, text),
    };

    // Breadcrumb before the handler runs, so a crash still records who ran it.
    if (this.persistLog || (this.apiUrl && this.apiKey)) {
      try {
        await this.persistLogEntry(id, {
          stdout: `[jobfy] picked up by worker "${this.workerName}" (attempt ${attempt})`,
          stderr: '',
          ranBy: this.workerName,
          kind: 'system',
        });
      } catch (err) {
        this.emitError(
          new Error(
            `Failed to persist pickup breadcrumb for ${id}: ${(err as Error).message}`,
          ),
        );
      }
    }

    let thrown: unknown = null;
    let result: unknown = undefined;
    try {
      result = await this.jobHandler(ctx);
    } catch (err) {
      thrown = err;
    } finally {
      this.running.delete(id);
    }

    // Flush all in-flight live-log writes before the job reaches a terminal
    // state, so the stream tail can't be lost when the consumer marks DONE.
    await Promise.allSettled(runningJob.pendingWrites);

    // Persist final aggregated buffers regardless of success/failure — the
    // standard worker does the same. ranBy lets the dashboard's worker filter
    // surface jobs that ran in this app. Skipped entirely when no persistence
    // is configured (the consumer handles it in its handler).
    const { stdout, stderr } = this.buffers.drain(id);
    if (this.persistLog || (this.apiUrl && this.apiKey)) {
      try {
        await this.persistLogEntry(id, {
          stdout,
          stderr,
          ranBy: this.workerName,
          kind: 'output',
        });
      } catch (err) {
        this.emitError(
          new Error(
            `Failed to persist log entry for ${id}: ${(err as Error).message}`,
          ),
        );
      }
    }

    if (thrown) {
      throw thrown instanceof Error ? thrown : new Error(String(thrown));
    }
    return result;
  }

  private async subscribeToCancellations(): Promise<void> {
    if (!this.subscriberClient) return;
    // Attach the listener BEFORE subscribing so a cancellation published in
    // the subscribe window isn't dropped.
    this.subscriberClient.on('message', (channel, message) => {
      if (channel !== CANCELLATIONS_CHANNEL) return;
      const running = this.running.get(message);
      if (!running) return;
      this.emitWarn(
        `cancellation received — ${this.formatRunning(this.describeRunning(message, running, Date.now()))}`,
      );
      running.controller.abort();
    });
    await this.subscriberClient.subscribe(CANCELLATIONS_CHANNEL);
  }

  private async appendToStream(
    id: string,
    attempt: number,
    stream: LogStream,
    text: string,
  ): Promise<void> {
    if (!this.commandClient) return;
    const key = `${EXEC_STREAM_KEY_PREFIX}${id}`;
    try {
      // Field order and key shape must match jobfy-api's ExecutionStreamService
      // exactly — that's what the dashboard's live-log SSE reads.
      await this.commandClient.xadd(
        key,
        'MAXLEN',
        '~',
        String(this.maxLogLines),
        '*',
        'attempt',
        String(attempt),
        'stream',
        stream,
        'text',
        text,
      );
      await this.commandClient.expire(key, EXEC_STREAM_TTL_SECONDS);
    } catch (err) {
      this.emitError(
        new Error(
          `Failed to XADD live-log chunk for ${id}: ${(err as Error).message}`,
        ),
      );
    }
  }

  private async persistLogEntry(id: string, entry: LogEntry): Promise<void> {
    const stamped: LogEntry = {
      ...entry,
      timestamp: entry.timestamp ?? new Date().toISOString(),
    };
    if (this.persistLog) {
      await this.persistLog(id, stamped);
      return;
    }
    const res = await fetch(`${this.apiUrl!}/executions/${id}/log`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.apiKey!,
      },
      body: JSON.stringify(stamped),
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }
  }

  private describeRunning(
    id: string,
    job: RunningJob,
    now: number,
  ): RunningJobInfo {
    return {
      id,
      name: job.name,
      attempt: job.attempt,
      startedAt: new Date(job.startedAt).toISOString(),
      runningForMs: now - job.startedAt,
      lastReportAgoMs: job.lastReportAt === null ? null : now - job.lastReportAt,
      lastReportText: job.lastReportText,
      aborted: job.controller.signal.aborted,
    };
  }

  private formatRunning(info: RunningJobInfo): string {
    const secs = (ms: number) => `${Math.round(ms / 1000)}s`;
    const last =
      info.lastReportAgoMs === null
        ? 'no report yet'
        : `last report ${secs(info.lastReportAgoMs)} ago: "${info.lastReportText}"`;
    return (
      `job ${info.id} "${info.name}" attempt ${info.attempt}, ` +
      `running ${secs(info.runningForMs)}${info.aborted ? ' (already aborted)' : ''}, ${last}`
    );
  }

  private emitWarn(message: string): void {
    if (this.warnHandler) {
      try {
        this.warnHandler(message);
      } catch {
        console.warn('[jobfy-lib] warn handler threw:', message);
      }
    } else {
      console.warn('[jobfy-lib]', message);
    }
  }

  private emitError(err: Error): void {
    if (this.errorHandler) {
      try {
        this.errorHandler(err);
      } catch {
        // user handler threw — last resort
        console.error('[jobfy-lib] error handler threw:', err);
      }
    } else {
      console.error('[jobfy-lib]', err);
    }
  }
}
