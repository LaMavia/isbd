import { fetch } from "bun";

const BASE = "http://localhost:8080";

const r = (await fetch(`${BASE}/queries`).then((r) => r.json())) as Record<
  string,
  string
>[];

const details = await Promise.all(
  r.map(async ({ queryId, status }) => {
    const [q, r, e] = await Promise.all([
      fetch(`${BASE}/query/${queryId}`).then((r) => r.json()),
      fetch(`${BASE}/result/${queryId}`).then((r) => r.json()),
      fetch(`${BASE}/error/${queryId}`).then((r) => r.json()),
    ]);

    return { queryId, status, q, r, e };
  }),
);

console.log(JSON.stringify(details, null, 2));
