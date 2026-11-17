import type { EnqueueRequest, Queue, QueueItem, QueueService } from './queue';

const reasonOf = (thrown: unknown): string =>
  thrown instanceof Error ? thrown.message : String(thrown);

export class QueueState {
  readonly #service: QueueService;
  #queues = $state.raw<readonly Queue[]>([]);
  #busy = $state(false);
  #loaded = $state(false);
  #problem = $state('');
  #flight: Promise<void> | undefined;

  constructor(service: QueueService) {
    this.#service = service;
  }

  get queues(): readonly Queue[] {
    return this.#queues;
  }

  get busy(): boolean {
    return this.#busy;
  }

  get loaded(): boolean {
    return this.#loaded;
  }

  get problem(): string {
    return this.#problem;
  }

  read(): Promise<void> {
    this.#flight ??= this.#read().finally(() => {
      this.#flight = undefined;
    });
    return this.#flight;
  }

  async #read(): Promise<void> {
    this.#busy = true;
    this.#problem = '';
    try {
      this.#queues = await this.#service.list();
      this.#loaded = true;
    } catch (thrown) {
      this.#problem = reasonOf(thrown);
    } finally {
      this.#busy = false;
    }
  }

  async enqueue(deviceId: string, request: EnqueueRequest): Promise<boolean> {
    return this.#mutate(async () => {
      await this.#service.enqueue(deviceId, request);
      await this.read();
    });
  }

  async cancel(item: QueueItem): Promise<boolean> {
    return this.#mutate(async () => {
      await this.#service.cancel(item.id);
      await this.read();
    });
  }

  async move(item: QueueItem, position: number): Promise<boolean> {
    return this.#mutate(async () => {
      await this.#service.move(item.id, position);
      await this.read();
    });
  }

  async #mutate(action: () => Promise<void>): Promise<boolean> {
    this.#busy = true;
    this.#problem = '';
    try {
      await action();
      return true;
    } catch (thrown) {
      this.#problem = reasonOf(thrown);
      return false;
    } finally {
      this.#busy = false;
    }
  }
}

let held: QueueState | undefined;

export const holdQueue = (queue: QueueState, signal: AbortSignal): void => {
  if (signal.aborted) return;
  held = queue;
  signal.addEventListener(
    'abort',
    () => {
      if (held === queue) held = undefined;
    },
    { once: true },
  );
};

export const heldQueue = (): QueueState => {
  if (!held) throw new Error('the print queue page rendered before setup');
  return held;
};
