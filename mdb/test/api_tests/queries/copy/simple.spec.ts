import { beforeAll, expect, test } from "bun:test";
import {
  assertDefined,
  awaitQuery,
  expectTrue,
  i64,
  readCsv,
  schema,
  setupClient,
  uniqueTableName,
  vc,
} from "../../common";
import {
  createTable,
  submitQuery,
  type ColumnReferenceExpression,
  type CopyQuery,
  type QueryResult,
  type SelectQuery,
} from "../../client";
import path from "path";

beforeAll(setupClient);

test.concurrent("loads", async () => {
  const name = uniqueTableName();
  const sourceFilepath = path.resolve(__dirname, "simple.csv");
  const doesCsvContainHeader = true;
  const columns = [i64("id"), vc("name"), vc("surname"), i64("age")];
  const csvData = await readCsv({
    path: sourceFilepath,
    header: doesCsvContainHeader,
    columns,
  });

  const {
    data: tableId,
    response: { status: tableStatus },
    error: tableError,
  } = await createTable({
    body: schema(name, columns),
  });

  expect(tableStatus).toBe(200);
  expect(tableError).toEqual(undefined);
  assertDefined(tableId);

  const {
    data: queryId,
    response: { status: submitQueryStatus },
    error: submitQueryError,
  } = await submitQuery({
    body: {
      queryDefinition: {
        destinationTableName: name,
        sourceFilepath,
        doesCsvContainHeader,
      } satisfies CopyQuery,
    },
  });

  expect(submitQueryError).toBeUndefined();
  expect(submitQueryStatus).toBe(200);
  assertDefined(queryId);

  {
    const q = await awaitQuery(queryId, 30_000);
    if (!q.ok) {
      console.error({ queryError: q.error });
    }

    expectTrue(q.ok, q.error);
    expect(q.result).toBeUndefined();
  }

  {
    const { data: queryId } = await submitQuery({
      body: {
        queryDefinition: {
          columnClauses: columns.map(
            ({ name: columnName }) =>
              ({
                columnName,
                tableName: name,
              }) satisfies ColumnReferenceExpression,
          ),
        } satisfies SelectQuery,
      },
    });

    assertDefined(queryId);

    const q = await awaitQuery(queryId, 30_000);
    const qresult = (await (
      q.result as any as ReadableStream
    ).json()) as QueryResult;

    expectTrue(q.ok, q.error);
    expect(qresult).toStrictEqual(csvData);
  }
});
