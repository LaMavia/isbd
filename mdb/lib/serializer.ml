(*
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


interface MDB {
  chunks: Chunk[];
  chunks_len: size_t;
  columns: Column[];
  columns_len: size_t;
}

type Column = `${i64}${string}${ColumnType}`
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

*)

module Make (IC : Cursor.CursorInterface) (OC : Cursor.CursorInterface) = struct
  let read_columns (input_cursor : IC.t) =
    let open IC in
    let offsets_len = 16 and input_len = len input_cursor in
    let offset_bytes =
      input_cursor |> seek (input_len - offsets_len) |> read offsets_len
    in
    let cols_offset = Bytes.get_int64_be offset_bytes 0 |> Int64.to_int
    and cols_lengths_offset =
      Bytes.get_int64_be offset_bytes 8 |> Int64.to_int
    in
    let cols_lens = cols_lengths_offset - cols_offset
    and cols_lengths_lens = input_len - cols_lengths_offset - offsets_len in
    let cols_bytes = input_cursor |> seek cols_offset |> read cols_lens in
    let cols_lengths_bytes = input_cursor |> read cols_lengths_lens in
    let bfs = Stateful_buffers.of_list [ cols_bytes; cols_lengths_bytes ] in
    Column.Deserializers.ColumnInfoDeserializer.deserialize_seq bfs 0
    |> List.of_seq

  let write_columns (logcols : (string * Column.col) list)
      (output_cursor : OC.t) =
    let open OC in
    let cols_offset = output_cursor |> position |> Int64.of_int in
    let max_total_cols_len, max_total_cols_lengths_len =
      List.fold_right
        (fun (s, _) (u, ul) -> (u + String.length s, ul + 9))
        logcols (0, 0)
    in
    let cols_bytes = Bytes.make max_total_cols_len '\000'
    and cols_lengths_bytes = Bytes.make max_total_cols_lengths_len '\000'
    and offsets_bytes = Bytes.make 16 '\000' in
    let bfs =
      Stateful_buffers.of_list [ cols_bytes; cols_lengths_bytes; offsets_bytes ]
    in
    logcols
    |> List.iter (fun logcol ->
           Column.Serializers.ColumnInfoSerializer.serialize logcol bfs 0);
    output_cursor
    |> write (Stateful_buffers.get_buffer bfs 0).position cols_bytes
    |> ignore;
    let cols_lengths_offset = output_cursor |> position |> Int64.of_int in
    Bytes.set_int64_be offsets_bytes 0 cols_offset;
    Bytes.set_int64_be offsets_bytes 8 cols_lengths_offset;
    output_cursor
    |> write (Stateful_buffers.get_buffer bfs 1).position cols_lengths_bytes
    |> ignore

  let serialize (buffer_size : int) (logcols : (string * Column.col) list)
      (_input_cursor : IC.t) (_output_cursor : OC.t) =
    let phys_lens =
      logcols
      |> List.map (function
           | _, `ColString -> Column.VarcharLogCol.physical_length
           | _, `ColInt -> Column.IntLogCol.physical_length)
    in
    let _bfs =
      let total_physcols = List.fold_right ( + ) phys_lens 0 in
      Stateful_buffers.create ~n:total_physcols ~len:buffer_size
    in
    ()
end
