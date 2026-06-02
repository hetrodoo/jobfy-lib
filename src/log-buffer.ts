import type { LogStream } from './types';

/**
 * In-memory accumulator for stdout/stderr keyed by job id. The Redis stream
 * captures lines as they happen for the live viewer, but the persisted
 * `execution.logs` entry needs aggregated buffers, so we keep a parallel
 * copy here and ship it to the API at the end of each attempt.
 */
export class LogBuffer {
  private readonly buffers = new Map<string, { stdout: string; stderr: string }>();

  append(id: string, stream: LogStream, text: string): void {
    let entry = this.buffers.get(id);
    if (!entry) {
      entry = { stdout: '', stderr: '' };
      this.buffers.set(id, entry);
    }
    // Mirror the standard worker's persisted format: each `report` becomes
    // one line in the aggregated buffer.
    entry[stream] += text.endsWith('\n') ? text : `${text}\n`;
  }

  drain(id: string): { stdout: string; stderr: string } {
    const entry = this.buffers.get(id) ?? { stdout: '', stderr: '' };
    this.buffers.delete(id);
    return entry;
  }
}
