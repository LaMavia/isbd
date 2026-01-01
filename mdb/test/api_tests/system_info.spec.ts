import { beforeAll, expect, test } from "bun:test";
import { setupClient } from "./common.ts";
import { getSystemInfo } from "./client/sdk.gen.ts";

beforeAll(setupClient);

test.concurrent("System info", async () => {
  const {
    response: { status },
    data: json,
  } = await getSystemInfo();

  expect(status).toBe(200);
  expect(json?.uptime).toBeGreaterThanOrEqual(0);
  expect(json?.author).toEqual("Zuzanna Surowiec");
});
