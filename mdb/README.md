# Format

```ts
interface MDB {
  chunks: Chunk[];
  chunks_len: size_t;
  columns: Column[];
  columns_len: size_t;
}

type Column = `${vle_uint}${string}${ColumnType}`
type ColumnType =
  | '\001' /* int      physical_columns = 1 */
  | '\002' /* varchar  physical_columns = 2 */
type vle_uint = /*  */;

interface Chunk {
  fragment_lengths: vle_uint[]; /* len(fragment_lengths) =
                                    sum(physical_columns(typeof(c)) for c in MDB.columns)
                                  */
  fragments: byte[];
}

/*
  Value Encodings:
  1. int: sign-bit vle_int
  2. varchar: string

  Fragment Encodings:
  1. int: identity
  2. varchar: lz4
*/
```

## Example

```ts
{
  chunks: [
    {
      fragment_lengths: [
        vle_uint.encode(3),
        vle_uint.encode(16),
        vle_uint.encode(4),
        vle_uint.encode(18),
        vle_uint.encode(4),
        vle_uint.encode(4)
      ].join(""),
      fragments: [
        enc_col_int(`${vle_int.encode(0)}${vle_int.encode(1)}${vle_int.encode(2)}`),
        enc_col_varchar(`ZuzannaKarolinaAnnaZofia`),
        enc_col_uint(`${vle_uint.encode(7)}${vle_uint.encode(7)}${vle_uint.encode(4)}${vle_uint.encode(5)}`),
        enc_col_varchar(`SurowiecKoszelaDilaiWeber`),
        enc_col_uint(`${vle_uint.encode(8)}${vle_uint.encode(7)}${vle_uint.encode(5)}${vle_uint.encode(5)}`),
        enc_col_int(`${vle_int.encode(23)}${vle_int.encode(22)}${vle_int.encode(26)}${vle_int.encode(25)}`)
      ].join("")
    }
  ] satisfies Chunk[],
  chunks_len: 1,
  columns: [
    `${vle_uint.encode(2)}id\001`,
    `${vle_uint.encode(4)}name\002`,
    `${vle_uint.encode(8)}last_name\002`,
    `${vle_uint.encode(3)}age\001`
  ] satisfies Column[],
  columns_len: 4,
} satisfies MDB
```

# Pseudocode

```ts
function serialize(
  logic_columns: LogicColumn[],
  source_stream: Stream<Data>,
  dist_stream: Stream<Chunk>,
) {
  /* as many as physical columns in logic_columns */
  allocate_buffers(logic_columns, buffers);
  const physical_lengths = logic_columns.map(
    (logcol) => logcol.physical_length,
  );

  for (const record of source_stream) {
    let buffer_i = 0;

    for (const [i, cell] of enumerate(record.columns)) {
      const logcol = logic_columns[i];

      logcol.serialize_mut(cell, buffers, buffer_i);
      buffer_i += physical_lengths[i];
    }

    if (should_dump_buffers(buffers)) {
      encode_fragments(logic_columns, buffers);
      emit_chunk(buffers, dist_stream);
      clear_buffers(buffers);
    }
  }

  if (!are_buffers_empty(buffers)) {
    encode_fragments(logic_columns, buffers);
    emit_chunk(buffers, dist_stream);
  }

  emit_footer(dist_stream);

  free_buffers(buffers);
}

function deserialize(
  logic_columns: LogicColumn[],
  source_stream: Stream<Chunk>,
  dist_stream: Stream<Data>,
) {
  /* max number of buffers neccessary for each logical column, so either 1 or 2 here */
  allocate_max_buffers(logic_columns, buffers);

  for (const chunk of source_stream) {
    const records = [];
    let fragment_lengths_i = 0;
    const fragment_lengths = new Array(physical_length)
      .fill(0)
      .map(() => vle_uint.decode_mut(chunk));

    for (const [i, logcol] of enumerate(logic_columns)) {
      logcol.load_mut(chunk, buffers, fragment_lengths, fragment_lengths_i);
      logcol.decode_fragments(buffers, fragment_lengths, fragment_lengths_i);

      for (const [i, cell] of enumerate(logcol.deserialize_iter(buffers))) {
        if (i >= records.length) {
          records.push(empty_record());
        }

        records[i].add_column(logcol, cell);
      }

      clear_buffers(buffers, logcol.physical_length);
      fragment_lengths_i += logcol.physical_columns;
    }

    for (const record of records) {
      emit_record(record, dist_stream);
    }
  }

  free_buffers(buffers);
}

interface LogicColumn {
  /** Load physical columns from chunk to buffers.
   * `fragment_lengths_i + i` is the length of the i-th physical column's fragment in `chunk`.
   */
  load_mut(
    chunk: Chunk,
    buffers: StatefulBuffers,
    fragment_lengths: size_t[],
    fragment_lengths_i: size_t,
  );

  /** Serialize the data in `cell` into buffers
   * `buffers[buffer_i + i]` for each physical column index `i`.
   */
  serialize_mut(cell: Data, buffers: StatefulBuffers, buffer_i: size_t): void;

  /** Decode (ie. decrypt) data in buffers `buffer[i]`
   * of lengths `fragment_lengths[fragment_lengths_i + i]`
   * for each physical column index `i`.
   */
  decode_fragments(
    buffers: StatefulBuffers,
    fragment_lengths: size_t[],
    fragment_lengths_i: size_t,
  );

  /** Deserialize data in `buffers` into the logical column.
   */
  deserialize_iter(buffers: StatefulBuffers): Iterator<Data>;

  /** Number of physical columns needed to represent the logical column.
   */
  physical_length: size_t;
}

type Data = { type: "int"; value: int64 } | { type: "varchar"; value: string };
```
