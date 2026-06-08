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
  LogStream,
  PersistLog,
} from './types';

interface JobData {
  command: string;
}

interface RunningJob {
  controller: AbortController;
  attempt: number;
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
    // BullMQ lock/stall tuning. Defaults match jobfy-api's worker and are sized
    // for long-running jobs with heavy log streaming: a generous lockDuration
    // absorbs event-loop bursts that would otherwise miss lock renewal.
    // maxStalledCount stays at 1 (BullMQ default) so a stalled job is not
    // silently re-run — raise it only for idempotent handlers.
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
  on(event: 'job' | 'error', handler: JobHandler | ErrorHandler): this {
    if (event === 'job') {
      this.jobHandler = handler as JobHandler;
    } else {
      this.errorHandler = handler as ErrorHandler;
    }
    return this;
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

    this.worker.on('error', (err) => this.emitError(err));

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

  private async persistLogEntry(
    id: string,
    entry: { stdout: string; stderr: string; ranBy: string },
  ): Promise<void> {
    if (this.persistLog) {
      await this.persistLog(id, entry);
      return;
    }
    const res = await fetch(`${this.apiUrl!}/executions/${id}/log`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.apiKey!,
      },
      body: JSON.stringify(entry),
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
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
