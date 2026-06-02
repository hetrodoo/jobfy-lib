# jobfy-lib

In-process Jobfy worker. Drop it into a Node.js app and let Jobfy dispatch jobs that run **in your process** instead of in a Docker container.

Use it when your workload is too lightweight to justify spinning up a container per execution, but you still want Jobfy's queueing, retries, live logs, cancellation, and dashboard observability.

## Install

This package isn't published. Consume it via a local path:

```json
{
  "dependencies": {
    "jobfy-lib": "file:../jobfy-lib"
  }
}
```

## Usage

```ts
import { Jobfy } from 'jobfy-lib';

const jobfy = new Jobfy({
  redisUrl: process.env.REDIS_URL!,           // same Redis the API uses
  queueName: 'my-app-jobs',                   // a dedicated queue (see warning below)
  apiUrl: 'https://jobfy-api.example.com',    // base URL of jobfy-api
  apiKey: process.env.JOBFY_API_KEY!,         // value of API_SECRET_KEY on the API
  workerName: 'my-app-worker-1',              // optional, defaults to os.hostname()
  concurrency: 4,                             // optional, defaults to 1
});

jobfy.on('job', async ({ id, command, signal, report }) => {
  report('stdout', `starting work on ${command}`);

  await doSomeWork({ signal });               // respect AbortSignal!

  report('stdout', 'done');
});

jobfy.on('error', (err) => {
  console.error('jobfy infra error:', err);
});

await jobfy.listen();

process.on('SIGTERM', () => void jobfy.close());
process.on('SIGINT', () => void jobfy.close());
```

## What you get

| Concern | How it works |
|---|---|
| **Job intake** | BullMQ Worker on the queue you configure. Same retries, backoff, concurrency knobs you'd configure on a standard worker. |
| **Status (`pending → running → done/failed`)** | Falls out of BullMQ automatically — the API's QueueEvents listener writes the transitions to Postgres. Your handler returning normally → `done`; throwing → `failed`. |
| **Live logs** | `ctx.report('stdout' \| 'stderr', text)` writes to the same Redis Stream the dashboard's live log viewer reads. |
| **Cancellation** | Click "cancel" in the dashboard → API publishes the id on the Redis cancellations channel → lib aborts the `AbortController` for that job. Respect `ctx.signal` in your handler. |
| **`ranBy` filter** | Whatever you pass as `workerName` shows up as `ranBy` on the dashboard's worker filter. |

## Important: your queue must be in the API's `JOB_QUEUES`

The lib does **not** write execution status itself. Status transitions
(`pending → running → done/failed`) are written by the API's `QueueEvents`
listener, which observes **every queue listed in the API's `JOB_QUEUES`**. So:

> **Your `queueName` MUST be in the API's `JOB_QUEUES` env, or executions will sit at `Pending` forever** — the job runs in your process, but nothing transitions its status.

This is the single most common setup mistake. The job processing, live logs,
and cancellation all work without it; only the status (and therefore the
dashboard's done/failed state) depends on the API observing the queue.

## Important: dedicated queue

The lib hands **every** job on its queue to your handler, regardless of the command prefix. If you mix it with a queue that's also processed by standard workers, you'll occasionally get `docker:` or `ssh:` jobs in your in-process handler — which won't know what to do with them.

**Always run jobfy-lib on a dedicated queue** and configure your producers to publish to that queue when they want in-process execution.

## Configuration

| Option | Required | Description |
|---|---|---|
| `redisUrl` | yes | Connection URL for the Redis the API + workers use. |
| `queueName` | yes | The BullMQ queue this lib consumes. Must be in `JOB_QUEUES` on the API. |
| `apiUrl` | conditional | HTTP base URL of jobfy-api. Used to POST the final log entry. Required only when relying on the HTTP fallback (no `persistLog`). |
| `apiKey` | conditional | Value of jobfy-api's `API_SECRET_KEY`. Sent as `x-api-key`. Required only with the HTTP fallback. |
| `persistLog` | no | `(id, { stdout, stderr, ranBy }) => void \| Promise<void>`. Custom final-log persistence; takes precedence over the HTTP fallback. The in-process jobfy worker uses this to write straight to Postgres. |
| `workerName` | no | Reported as `ranBy`. Defaults to `os.hostname()`. |
| `concurrency` | no | BullMQ Worker concurrency. Defaults to `1`. |
| `maxLogLines` | no | Cap on live-log stream entries (`MAXLEN ~`). Defaults to `250`. |

If you provide neither `persistLog` nor `apiUrl`/`apiKey`, the lib skips final-log persistence entirely — the consumer is expected to persist inside its handler. (That's what jobfy-api's own worker does: it reuses this engine but writes logs directly to the database.)

The handler receives `{ id, name, command, signal, report }` and whatever it returns becomes the BullMQ job's return value (stored as the execution's result).

## What it doesn't do

- It doesn't execute `docker:` or `ssh:` commands — those are the standard worker's job. See the warning above about dedicated queues.
- It doesn't write to Postgres directly. The final `{stdout, stderr, ranBy}` entry is POSTed to `jobfy-api`'s `POST /executions/:id/log` endpoint.
- It doesn't enforce a wall-clock timeout. Use `setTimeout` + `controller.abort()` in your handler if you need one.
- It doesn't kill your handler. If you ignore `ctx.signal`, the job stays alive — your problem.
