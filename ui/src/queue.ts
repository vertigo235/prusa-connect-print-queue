import { bind, type components } from 'print-queue-client';
import type { ModuleHost } from '@prusa/connect-ui/slots';

export type Queue = components['schemas']['Queue'];
export type QueueItem = components['schemas']['QueueItem'];
export type EnqueueRequest = components['schemas']['EnqueueRequest'];

export interface QueueService {
  list(signal?: AbortSignal): Promise<readonly Queue[]>;
  enqueue(deviceId: string, request: EnqueueRequest, signal?: AbortSignal): Promise<QueueItem>;
  cancel(id: string, signal?: AbortSignal): Promise<QueueItem>;
  move(id: string, position: number, signal?: AbortSignal): Promise<QueueItem>;
}

export const queueOn = (host: ModuleHost): QueueService => {
  const routes = bind(host.client);

  return {
    list: async (signal) => (await routes.listQueues({}, signal)).queues,
    enqueue: async (deviceId, request, signal) =>
      routes.enqueuePrint({ path: { device_id: deviceId }, body: request }, signal),
    cancel: async (id, signal) =>
      routes.cancelQueueItem({ path: { id } }, signal),
    move: async (id, position, signal) =>
      routes.moveQueueItem({ path: { id }, body: { position } }, signal),
  };
};
