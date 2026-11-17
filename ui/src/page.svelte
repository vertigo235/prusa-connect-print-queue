<script lang="ts">
  import {
    Badge,
    Button,
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    Input,
    Label,
  } from '@prusa/connect-ui/primitives';
  import { parseFileRef } from '@prusa/connect-ui/slots';
  import { onMount } from 'svelte';

  import { heldHost } from './host';
  import { heldQueue } from './queue.svelte';
  import type { EnqueueRequest, Queue, QueueItem } from './queue';

  const host = heldHost();
  const t = host.t;
  const queue = heldQueue();
  const uid = $props.id();
  let { deviceId }: { deviceId?: string } = $props();
  type Typed = Event & { currentTarget: HTMLInputElement };

  let storage = $state<Record<string, string>>({});
  let source = $state<Record<string, string>>({});
  let printerPath = $state<Record<string, string>>({});
  let destination = $state<Record<string, string>>({});

  const stateOf = (one: Queue): string => {
    const raw = one.device.state?.['/state'];
    return typeof raw === 'string' ? raw : 'UNKNOWN';
  };

  const visibleQueues = $derived(
    deviceId
      ? queue.queues.filter((one) => one.device.id === deviceId)
      : queue.queues,
  );

  const pendingOf = (one: Queue): readonly QueueItem[] =>
    one.items.filter((item) => item.state === 'queued');

  const activeOf = (one: Queue): QueueItem | undefined =>
    one.items.find((item) => item.state === 'starting' || item.state === 'printing');

  const terminalOf = (one: Queue): readonly QueueItem[] =>
    one.items.filter(
      (item) => item.state === 'done' || item.state === 'failed' || item.state === 'cancelled',
    );

  const set = (values: Record<string, string>, id: string, value: string): void => {
    values[id] = value;
  };

  const add = async (event: SubmitEvent, deviceId: string): Promise<void> => {
    event.preventDefault();
    const sourcePath = source[deviceId]?.trim() ?? '';
    const storageName = storage[deviceId]?.trim() ?? '';
    const printerFile = printerPath[deviceId]?.trim() ?? '';
    if ((!sourcePath || !storageName) && !printerFile) return;

    const request: EnqueueRequest = printerFile
      ? { printer_path: printerFile }
      : { storage: storageName, source: sourcePath };
    const destinationPath = destination[deviceId]?.trim();
    if (destinationPath) request.destination = destinationPath;
    const added = await queue.enqueue(deviceId, request);
    if (added) {
      storage[deviceId] = '';
      source[deviceId] = '';
      printerPath[deviceId] = '';
      destination[deviceId] = '';
    }
  };

  const cancel = (item: QueueItem): void => {
    void queue.cancel(item);
  };

  const move = (items: readonly QueueItem[], item: QueueItem, offset: number): void => {
    const index = items.indexOf(item);
    const target = index + offset;
    if (index < 0 || target < 0 || target >= items.length) return;
    void queue.move(item, target);
  };

  const prefill = (): void => {
    const fileRef = new URLSearchParams(window.location.search).get('file');
    if (!fileRef) return;
    const file = parseFileRef(fileRef);
    if (!file) return;
    const printerSeparator = file.storage === 'printer' ? file.path.indexOf(':') : -1;
    const printerDevice = printerSeparator < 0 ? '' : file.path.slice(0, printerSeparator);
    const printerSource = printerSeparator < 0 ? '' : file.path.slice(printerSeparator + 1);
    for (const one of visibleQueues) {
      if (printerDevice && one.device.id !== printerDevice) continue;
      if (printerSource) {
        printerPath[one.device.id] = printerSource;
      } else {
        storage[one.device.id] = file.storage;
        source[one.device.id] = file.path;
      }
    }
  };

  const read = async (): Promise<void> => {
    await queue.read();
    prefill();
  };

  onMount(() => {
    void read();
    window.addEventListener('popstate', prefill);
    const timer = setInterval(() => void queue.read(), 5000);
    return () => {
      window.removeEventListener('popstate', prefill);
      clearInterval(timer);
    };
  });
</script>

<section class="flex flex-col gap-4">
  <header class="flex flex-wrap items-center justify-between gap-3">
    <h1 class="text-lg font-semibold">{t('print-queue.page.title')}</h1>
    <Button variant="secondary" size="sm" disabled={queue.busy} onclick={() => void queue.read()}>
      {t('print-queue.page.refresh')}
    </Button>
  </header>

  <p aria-live="polite" class="text-sm text-destructive-text">{queue.problem}</p>

  {#if !queue.loaded}
    <Card>
      <CardContent><p class="text-muted-foreground">{t('print-queue.page.reading')}</p></CardContent>
    </Card>
  {:else if visibleQueues.length === 0}
    <Card>
      <CardContent><p class="text-muted-foreground">{t('print-queue.page.empty')}</p></CardContent>
    </Card>
  {:else}
    {#each visibleQueues as one (one.device.id)}
      {@const pending = pendingOf(one)}
      {@const active = activeOf(one)}
      <Card>
        <CardHeader>
          <div class="flex flex-wrap items-center justify-between gap-2">
            <CardTitle>{one.device.name || one.device.id}</CardTitle>
            <Badge variant={one.device.connected ? 'success' : 'secondary'}>
              {one.device.connected ? t('print-queue.page.connected') : t('print-queue.page.offline')}
            </Badge>
          </div>
          <p class="text-sm text-muted-foreground">
            {t('print-queue.page.state', { state: stateOf(one) })}
          </p>
        </CardHeader>
        <CardContent class="flex flex-col gap-4">
          {#if active}
            <div class="rounded border border-border p-3">
              <div class="flex flex-wrap items-center gap-2">
                <Badge variant="info">{t('print-queue.page.active')}</Badge>
                <span class="font-medium">{active.display_name}</span>
                <span class="text-sm text-muted-foreground">{active.state}</span>
              </div>
              {#if active.error}<p class="mt-1 text-sm text-destructive-text">{active.error}</p>{/if}
            </div>
          {/if}

          {#if pending.length === 0}
            <p class="text-sm text-muted-foreground">{t('print-queue.page.noPending')}</p>
          {:else}
            <ol class="flex flex-col gap-2">
              {#each pending as item, index (item.id)}
                <li class="flex flex-wrap items-center gap-2 border-b border-border pb-2">
                  <span class="w-7 text-sm text-muted-foreground">{index + 1}</span>
                  <span class="min-w-32 font-medium">{item.display_name}</span>
                  <Badge variant="secondary">{item.submitted_by}</Badge>
                  {#if item.error}<span class="text-sm text-destructive-text">{item.error}</span>{/if}
                  <span class="ml-auto flex gap-1">
                    <Button
                      variant="ghost"
                      size="sm"
                      type="button"
                      disabled={queue.busy || index === 0}
                      onclick={() => move(pending, item, -1)}
                    >{t('print-queue.page.up')}</Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      type="button"
                      disabled={queue.busy || index === pending.length - 1}
                      onclick={() => move(pending, item, 1)}
                    >{t('print-queue.page.down')}</Button>
                    <Button
                      variant="destructive"
                      size="sm"
                      type="button"
                      disabled={queue.busy}
                      onclick={() => cancel(item)}
                    >{t('print-queue.page.delete')}</Button>
                  </span>
                </li>
              {/each}
            </ol>
          {/if}

          {@const history = terminalOf(one)}
          {#if history.length > 0}
            <section class="flex flex-col gap-2 border-t border-border pt-3">
              <h3 class="text-sm font-medium">{t('print-queue.page.history')}</h3>
              <ul class="flex flex-col gap-1 text-sm">
                {#each history as item (item.id)}
                  <li class="flex flex-wrap items-center gap-2">
                    <span class="font-medium">{item.display_name}</span>
                    <Badge variant={item.state === 'failed' ? 'destructive' : 'secondary'}>{item.state}</Badge>
                    {#if item.error}<span class="text-muted-foreground">{item.error}</span>{/if}
                  </li>
                {/each}
              </ul>
            </section>
          {/if}

          <form class="grid gap-3 @md:grid-cols-2" onsubmit={(event) => void add(event, one.device.id)}>
            {#if printerPath[one.device.id]}
              <div class="flex flex-col gap-1 @md:col-span-2">
                <Label for={`${uid}-${one.device.id}-printer-file`}>{t('print-queue.page.printerFile')}</Label>
                <Input
                  id={`${uid}-${one.device.id}-printer-file`}
                  value={printerPath[one.device.id]}
                  readonly
                />
              </div>
            {:else}
              <div class="flex flex-col gap-1">
                <Label for={`${uid}-${one.device.id}-storage`}>{t('print-queue.page.storage')}</Label>
                <Input
                  id={`${uid}-${one.device.id}-storage`}
                  value={storage[one.device.id] ?? ''}
                  oninput={(event: Typed) => set(storage, one.device.id, event.currentTarget.value)}
                  placeholder={t('print-queue.page.storagePlaceholder')}
                  required
                />
              </div>
              <div class="flex flex-col gap-1">
                <Label for={`${uid}-${one.device.id}-source`}>{t('print-queue.page.source')}</Label>
                <Input
                  id={`${uid}-${one.device.id}-source`}
                  value={source[one.device.id] ?? ''}
                  oninput={(event: Typed) => set(source, one.device.id, event.currentTarget.value)}
                  placeholder={t('print-queue.page.sourcePlaceholder')}
                  required
                />
              </div>
            {/if}
            <div class="flex flex-col gap-1">
              <Label for={`${uid}-${one.device.id}-destination`}>{t('print-queue.page.destination')}</Label>
              <Input
                id={`${uid}-${one.device.id}-destination`}
                value={destination[one.device.id] ?? ''}
                oninput={(event: Typed) => set(destination, one.device.id, event.currentTarget.value)}
                placeholder={t('print-queue.page.destinationPlaceholder')}
              />
            </div>
            <div class="flex items-end">
              <Button
                type="submit"
                disabled={queue.busy || (!printerPath[one.device.id]?.trim() && (!storage[one.device.id]?.trim() || !source[one.device.id]?.trim()))}
              >
                {t('print-queue.page.add')}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    {/each}
  {/if}
</section>
