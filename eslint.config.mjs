import tseslint from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";

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
      "@typescript-eslint": tseslint
    },
    rules: {
      ...tseslint.configs["flat/eslint-recommended"].rules,
      ...tseslint.configs["flat/recommended"].rules,
      "@typescript-eslint/explicit-function-return-type": "off",
      "@typescript-eslint/no-explicit-any": "warn",
      "@typescript-eslint/no-unused-vars": ["warn", { "argsIgnorePattern": "^_" }],
      "@typescript-eslint/no-inferrable-types": "off",
      "no-console": "warn",
      "semi": ["error", "always"],
      "quotes": ["error", "double"]
    }
  },
  {
    ignores: ["dist/**", "node_modules/**", "coverage/**"]
  }
];
