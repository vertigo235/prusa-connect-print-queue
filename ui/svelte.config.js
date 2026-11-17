import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

export default {
  preprocess: vitePreprocess(),
  // Runes only. A legacy component fails to compile rather than running in a
  // mode this module never tests.
  compilerOptions: { runes: true },
};
