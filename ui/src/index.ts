// The bundle's entry. The app imports this file and calls what it default
// exports, handing it the one object a module receives.
//
// `defineModule` is an identity function. It exists so an author gets the
// host's types at this call site without importing the host.
import { defineModule } from '@prusa/connect-ui/slots';

import { holdHost } from './host';
import { holdQueue, QueueState } from './queue.svelte';
import { queueOn } from './queue';
import Page from './page.svelte';
import './styles.css';

export default defineModule((host) => {
  // Everything a component of this module needs is built here, from the host
  // and from nothing else. The host arrives before any component is mounted,
  // so it is held rather than threaded down.
  holdHost(host);
  holdQueue(new QueueState(queueOn(host)), host.signal);

  // host.signal fires when the person who signed in leaves. Every request of
  // host.client ends with it. Pass it to a call that takes a signal, and stop
  // every timer and stream you start when it fires, for example:
  //
  //   const timer = setInterval(refresh, 5000);
  //   host.signal.addEventListener('abort', () => clearInterval(timer));
  //   await client.listThings({}, host.signal);

  // Every registration below is held to `contributes` in module.yaml: a name
  // the manifest does not declare is refused at run time and the app drops
  // this module. The generator wrote both from one answer, so they agree; keep
  // them in step by hand after that.
  host.register.page('print-queue', Page);
  host.register.deviceTab('queue', Page);
  host.register.fileAction('enqueue', (fileRef) => {
    const href = `${host.pageHref('print-queue')}?file=${encodeURIComponent(fileRef)}`;
    window.history.pushState(null, '', href);
    window.dispatchEvent(new PopStateEvent('popstate'));
    return Promise.resolve();
  });
});
