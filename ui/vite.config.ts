import { copyFileSync, cpSync, mkdirSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { moduleBuild, moduleDev } from '@prusa/connect-ui/build';

// The bundle the host serves. `moduleBuild()` makes the choices every module
// makes, so the only thing written here is where the files go.
//
// The bundle root is `dist/`. The manifest sits at its top and the entry is
// `dist/ui/index.js`, because `module.yaml` declares `entry: ui/index.js` and
// the host reads that name relative to the bundle root.
//
// Nothing here imports `vite`. `moduleBuild()` and `moduleDev()` are typed
// against the library's own copy, and a second copy in this project's
// `node_modules` is a different type to the compiler, so the default export
// below is a plain function rather than `defineConfig`: Vite calls it with
// the command and reads back whichever config object it returns.
const BUNDLE_ROOT = fileURLToPath(new URL('./dist/', import.meta.url));
const MANIFEST = fileURLToPath(new URL('../module.yaml', import.meta.url));
const LOCALES = fileURLToPath(new URL('./src/locales/', import.meta.url));

/**
 * Puts the module's own manifest at the root of the bundle, which is where the
 * host reads it from. The digest covers the whole directory, so the manifest
 * the host reads is the one the install recorded.
 *
 * The directory is cleared first rather than written over: the digest counts
 * every file in it, so a file left behind by an earlier build would be part of
 * what the install records and what the host serves.
 */
const bundleManifest = () => ({
  name: 'print-queue:bundle-manifest',
  buildStart: {
    order: 'pre' as const,
    handler: () => {
      rmSync(BUNDLE_ROOT, { recursive: true, force: true });
      mkdirSync(BUNDLE_ROOT, { recursive: true });
    },
  },
  closeBundle: {
    order: 'post' as const,
    handler: () => {
      copyFileSync(MANIFEST, `${BUNDLE_ROOT}module.yaml`);
      // The catalogues, beside the entry. The host reads
      // `locales/<lang>.json` next to `entry_url` and merges what it finds
      // under this module's namespace (frontend-i18n.md rule 7).
      cpSync(LOCALES, `${BUNDLE_ROOT}ui/locales`, { recursive: true });
    },
  },
});

const bundle = moduleBuild({ entry: 'src/index.ts', outDir: 'dist/ui' });

// `pnpm dev` develops the module behind its own dev server, in front of the
// core `PCL_CORE_URL` names (ADR-20260924-5nx). `pnpm build` writes the release.
export default ({ command }: { command: 'build' | 'serve' }) =>
  command === 'serve'
    ? moduleDev()
    : { ...bundle, plugins: [...(bundle.plugins ?? []), bundleManifest()] };
