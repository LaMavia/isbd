import { beforeAll, expect, test } from "bun:test";
import { setupClient, uniqueTableName, uuidRegex } from "./common.ts";
import {
  createTable,
  deleteTable,
  getTableById,
  getTables,
} from "./client/sdk.gen.ts";

type Column = { name: string; type: "INT64" | "VARCHAR" };
type TableSchema = { name: string; columns: Column[] };

beforeAll(setupClient);

test.concurrent("CRUD", async () => {
  const name = uniqueTableName();
  const schema = {
    name,
    columns: [
      {
        name: "a",
        type: "INT64",
      },
      { name: "b", type: "VARCHAR" },
    ],
  } satisfies TableSchema;

  const {
    data: tableId,
    response: { status: putStatus },
  } = await createTable({
    body: schema,
  });

  expect(putStatus).toBe(200);
  expect(tableId).toBeTypeOf("string");
  expect(tableId).toMatch(uuidRegex);

  const {
    data: tableData,
    response: { status: getStatus },
  } = await getTableById({
    path: {
      tableId: tableId!,
    },
  });

  expect(getStatus).toBe(200);
  expect(tableData).toStrictEqual(tableData);

  const {
    data: tables,
    response: { status: getTablesStatus },
  } = await getTables();
  expect(getTablesStatus).toBe(200);
  expect(tables).toBeDefined();
  expect(tables).toContainEqual({ name, tableId });

  const {
    error: createError,
    response: { status: createErrorStatus },
  } = await createTable({ body: schema });
  expect(createErrorStatus).toBe(400);
  expect(createError?.problems?.length).toBeGreaterThan(0);

  const {
    response: { status: deleteStatus },
  } = await deleteTable({ path: { tableId: tableId! } });
  expect(deleteStatus).toBe(200);

  const {
    response: { status: notFoundStatus },
  } = await getTableById({ path: { tableId: tableId! } });
  expect(notFoundStatus).toBe(404);
});

test.concurrent("Invalid create", async () => {
  const name = uniqueTableName();
  const {
    error,
    response: { status },
  } = await createTable({
    body: {
      name,
      columns: [
        //@ts-ignore
        { name: "col", type: "5" },
      ],
    },
  });

  expect(status).toBe(400);
  expect(error).toBeDefined();
  expect(error?.problems?.length).toBeGreaterThan(0);

  {
    const { data: tables } = await getTables();
    expect(tables).toBeDefined();
    expect(tables?.find((x) => x.name === name)).toBeUndefined();
  }
});
