import { defineConfig } from "vite-plus";
import { resolve } from "path";

export default defineConfig(({ command: _command }) => ({
  fmt: {
    printWidth: 80,
    sortPackageJson: false,
    ignorePatterns: [
      ".agent/**",
      ".agents/**",
      ".claude/**",
      ".codex/**",
      ".continue/**",
      ".cursor/**",
      ".gemini/**",
      ".opencode/**",
      ".pi/**",
      ".roo/**",
      ".windsurf/**",
      // npm run format only covers TypeScript. Keep markdown and YAML out of vp check.
      "**/*.md",
      "**/*.yml",
      "**/*.yaml",
    ],
  },
  lint: {
    plugins: [],
    categories: {
      correctness: "off",
    },
    env: {
      builtin: true,
    },
    ignorePatterns: [
      "dist/**",
      "node_modules/**",
      "*.config.js",
      "coverage/**",
      ".agent/**",
      ".agents/**",
      ".claude/**",
      ".codex/**",
      ".continue/**",
      ".cursor/**",
      ".gemini/**",
      ".opencode/**",
      ".pi/**",
      ".roo/**",
      ".windsurf/**",
    ],
    jsPlugins: [
      {
        name: "vite-plus",
        specifier: "vite-plus/oxlint-plugin",
      },
    ],
    overrides: [
      {
        files: ["**/*.ts", "**/*.mts"],
        rules: {
          "no-array-constructor": "error",
          "no-unused-expressions": "error",
          "no-unused-vars": [
            "error",
            {
              argsIgnorePattern: "^_",
            },
          ],
          "typescript/ban-ts-comment": "error",
          "typescript/consistent-type-imports": "error",
          "typescript/no-duplicate-enum-values": "error",
          "typescript/no-empty-object-type": "error",
          "typescript/no-explicit-any": "warn",
          "typescript/no-extra-non-null-assertion": "error",
          "typescript/no-misused-new": "error",
          "typescript/no-namespace": "error",
          "typescript/no-non-null-asserted-optional-chain": "error",
          "typescript/no-require-imports": "error",
          "typescript/no-this-alias": "error",
          "typescript/no-unnecessary-type-constraint": "error",
          "typescript/no-unsafe-declaration-merging": "error",
          "typescript/no-unsafe-function-type": "error",
          "typescript/no-wrapper-object-types": "error",
          "typescript/prefer-as-const": "error",
          "typescript/prefer-namespace-keyword": "error",
          "typescript/triple-slash-reference": "error",
          "typescript/explicit-function-return-type": "off",
          "typescript/explicit-module-boundary-types": "off",
          "oxc/no-accumulating-spread": "error",
        },
        plugins: ["typescript", "oxc"],
        env: {
          node: true,
        },
      },
    ],
    options: {
      typeAware: true,
      typeCheck: true,
    },
    rules: {
      "vite-plus/prefer-vite-plus-imports": "error",
    },
  },
  // Build configuration
  build: {
    lib: {
      entry: resolve(__dirname, "src/index.ts"),
      name: "MCPTypescriptTemplate",
      fileName: "index",
      formats: ["es"],
    },
    target: "node24",
    outDir: "dist",
    emptyOutDir: true,
    ssr: true,
  },
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"),
    },
  },
  ssr: {
    external: [
      "@modelcontextprotocol/server",
      "@modelcontextprotocol/node",
      "express",
      "effect",
    ],
  },
}));
