// The host object this module was handed, held for its components to read.
//
// A module-level holder and not Svelte context: the setup function runs before
// any component of this module is mounted, so there is no component for
// `setContext` to hang on. It is set once, in `index.ts`, before anything can
// be drawn.
//
// What a component reads off it is `host.t`, the translate function the loader
// bound to this module's namespace. Every text this module draws comes through
// it, against src/locales/en.json (frontend-i18n.md rule 7).
import type { ModuleHost } from '@prusa/connect-ui/slots';

let held: ModuleHost | undefined;

/**
 * Called once by the module's setup function, with the host it was given. The
 * host belongs to the person who admitted this module. When its signal fires,
 * that person has left, so the host is let go and the next setup holds a new
 * one.
 */
export const holdHost = (host: ModuleHost): void => {
  // A setup that was still running when the person left resumes with a
  // signal that already fired. Its host is not held.
  if (host.signal.aborted) return;
  held = host;
  host.signal.addEventListener(
    'abort',
    () => {
      if (held === host) held = undefined;
    },
    { once: true },
  );
};

/** The host a component reads. It throws rather than drawing an empty view. */
export const heldHost = (): ModuleHost => {
  if (!held) {
    throw new Error('the print-queue module drew before its setup ran');
  }

  return held;
};
