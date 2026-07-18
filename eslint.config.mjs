import tseslint from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";
import unusedImports from "eslint-plugin-unused-imports";

export default [
  {
    files: ["src/**/*.ts"],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        project: "./tsconfig.json",
        sourceType: "module"
      }
    },
    plugins: {
      "@typescript-eslint": tseslint,
      "unused-imports": unusedImports
    },
    rules: {
      ...tseslint.configs["flat/eslint-recommended"].rules,
      ...tseslint.configs["flat/recommended"].rules,
      "@typescript-eslint/explicit-function-return-type": "off",
      "@typescript-eslint/no-explicit-any": "warn",
      "@typescript-eslint/no-unused-vars": "off",
      "unused-imports/no-unused-imports": "error",
      "unused-imports/no-unused-vars": ["error", { "argsIgnorePattern": "^_", "varsIgnorePattern": "^_" }],
      "@typescript-eslint/no-inferrable-types": "off",
      "no-console": "warn",
      "semi": ["error", "always"],
      "quotes": ["error", "double"],
      "brace-style": ["error", "allman", { "allowSingleLine": false }],
      "no-inline-comments": ["error", { "ignorePattern": "^(TODO|FIXME|eslint)" }]
    }
  },
  {
    ignores: ["dist/**", "node_modules/**", "coverage/**"]
  }
];
