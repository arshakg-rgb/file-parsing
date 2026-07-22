import { defineConfig } from "vitest/config";
import path from "path";

export default defineConfig({
  test: {
    environment: "node",
    globals: true,
    include: ["src/**/*.test.ts", "src/**/*.spec.ts"],
  },
  resolve: {
    alias: {
      "@common": path.resolve(__dirname, "src/common"),
      "@enum": path.resolve(__dirname, "src/enum"),
      "@utils": path.resolve(__dirname, "src/utils"),
      "@config": path.resolve(__dirname, "src/config"),
      "@errors": path.resolve(__dirname, "src/errors"),
      "@middleware": path.resolve(__dirname, "src/middleware"),
      "@api": path.resolve(__dirname, "src/api"),
      "@routes": path.resolve(__dirname, "src/routes"),
      "@shared": path.resolve(__dirname, "src/shared"),
      "@service": path.resolve(__dirname, "src/services"),
      "@scripts": path.resolve(__dirname, "src/scripts"),
      "@db": path.resolve(__dirname, "src/db"),
    },
  },
});
