import { moduleConfig } from '@prusa/frontend-standards/eslint/module';

import svelteConfig from './svelte.config.js';

export default moduleConfig({
  tsconfigRootDir: import.meta.dirname,
  svelteConfig,
});
