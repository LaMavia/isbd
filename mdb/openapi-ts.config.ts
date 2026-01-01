import { defineConfig } from "@hey-api/openapi-ts";

export default defineConfig({
  input:
    "https://raw.githubusercontent.com/smogork/ISBD-MIMUW/refs/heads/main/interface/dbmsInterface.yaml",
  output: "test/api_tests/client",
  plugins: [
    // "zod",
    {
      name: "@hey-api/sdk",
      validator: false,
    },
  ],
});
