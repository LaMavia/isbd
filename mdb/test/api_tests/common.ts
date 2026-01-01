import { env, randomUUIDv7, sleep } from "bun";
import { expect } from "bun:test";
import { client } from "./client/client.gen";
import {
  getQueryById,
  getQueryError,
  getQueryResult,
  type Column,
  type MultipleProblemsError,
  type Query,
  type QueryId,
  type QueryResult,
  type TableSchema,
} from "./client";
import parse from "csv-simple-parser";

const str = <T extends Record<string, any>>(q: T) =>
  `?${Object.values(q)
    .map((k, v) => `${k}=${encodeURIComponent(v)}`)
    .join("&")}`;

export const api = <T extends any[]>(
  fragments: TemplateStringsArray,
  ...args: T
) =>
  fragments
    .slice(1)
    .reduce(
      (u, f, i) =>
        `${u}${typeof args[i] === "object" ? str(args[i]) : args[i]}${f}`,
      `http://localhost:8080/${fragments[0]}`,
    );

export const uniqueTableName = () => `table${randomUUIDv7("hex")}`;

export const uuidRegex =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const setupClient = () => {
  client.setConfig({
    baseUrl: env["BASE_URL"],
  });
};

export const i64 = (name: string): Column => ({ name, type: "INT64" });
export const vc = (name: string): Column => ({ name, type: "VARCHAR" });

export const schema = (name: string, columns: Column[]): TableSchema => ({
  name,
  columns,
});

/** timeout in ms */
export const awaitQuery = (
  queryId: QueryId,
  timeout: number,
): Promise<
  { query: Query; result?: QueryResult; error?: MultipleProblemsError } & (
    | { ok: true; result: QueryResult }
    | { ok: false; error: MultipleProblemsError }
  )
> =>
  new Promise(async (res, rej) => {
    let shouldExit = false;
    const controller = new AbortController();

    const timeoutTimerId = setTimeout(() => {
      shouldExit = true;
      controller.abort("Timeout");
      rej("Timeout");
    }, timeout);

    while (!shouldExit) {
      const {
        data: query,
        response,
        error,
      } = await getQueryById({
        path: { queryId },
        signal: controller.signal,
      });

      if (response.status >= 400 || query === undefined) {
        clearTimeout(timeoutTimerId);
        rej(error);
        return;
      }

      switch (query.status) {
        case "COMPLETED": {
          clearTimeout(timeoutTimerId);
          res({
            query,
            ok: true,
            result: (await getQueryResult({ path: { queryId } })).data!,
          });
          return;
        }
        case "FAILED": {
          clearTimeout(timeoutTimerId);
          res({
            query,

            ok: false,
            error: (await getQueryError({ path: { queryId } })).data!,
          });
          return;
        }
        default:
          await sleep(500);
          break;
      }
    }
  });

export function assertDefined<T>(v?: T): asserts v is T {
  expect(v).toBeDefined();
}

export function expectTrue(v: boolean | undefined, m: any): asserts v is true {
  if (!v) {
    console.error(m);
  }
  expect(v).toBe(true);
}

export function expectFalse(
  v: boolean | undefined,
  m: any,
): asserts v is false {
  if (v) {
    console.error(m);
  }
  expect(v).toBe(false);
}

export const readCsv = async ({
  path,
  columns,
  header,
}: {
  path: string;
  columns: Column[];
  header: boolean;
}): Promise<QueryResult> => {
  const f = Bun.file(path);
  const result = {
    rowCount: 0,
    columns: new Array(columns.length).fill(undefined).map(() => []) as Exclude<
      QueryResult[number]["columns"],
      undefined
    >,
  } satisfies QueryResult[number];
  const csv = parse(await f.text(), { header });

  for (const row of csv) {
    if (Object.values(row).length !== columns.length) {
      continue;
    }

    result.rowCount++;

    for (const colIdx in columns) {
      let c;
      let v;
      if (Array.isArray(row)) {
        c = result.columns[colIdx]! as any[];
        v = row[colIdx] as number | string;
      } else {
        const colName = columns[colIdx]!.name;
        c = result.columns[colIdx]! as any[];
        v = row[colName] as number | string;
      }

      c.push(Number.isInteger(+v) ? +v : v);
    }
  }

  return [result];
};
