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

module Make (OC : Cursor.CursorInterface) = struct
  let write_columns (logcols : (string * Column.col) list)
      (output_cursor : OC.t) =
    let open OC in
    let cols_offset = output_cursor |> position |> Int64.of_int in
    let max_total_cols_len, max_total_cols_lengths_len =
      List.fold_right
        (fun (s, _) (u, ul) -> (u + String.length s + 1, ul + 9))
        logcols (0, 0)
    in
    let cols_bytes = Stateful_buffers.create_bytes max_total_cols_len
    and cols_lengths_bytes =
      Stateful_buffers.create_bytes max_total_cols_lengths_len
    and offsets_bytes = Stateful_buffers.create_bytes 16 in
    let bfs =
      Stateful_buffers.of_list [ cols_bytes; cols_lengths_bytes; offsets_bytes ]
    in
    (* Serialize each column *)
    logcols
    |> List.iter (fun logcol ->
           Column.Serializers.ColumnInfoSerializer.serialize logcol bfs 0);
    Column.Deserializers.ColumnInfoDeserializer.encode_fragments bfs 0;
    let cols_bf = Stateful_buffers.get_buffer bfs 0 in
    let cols_lengths_bf = Stateful_buffers.get_buffer bfs 1 in
    Utils.Debugging.print_hex_bytes "[@serializer] cols_bf after" cols_bf.buffer;
    Utils.Debugging.print_hex_bytes "[@serializer] cols_lengths_bf after"
      cols_lengths_bf.buffer;
    (* Dump column bytes *)
    output_cursor |> write cols_bf.position cols_bf.buffer |> ignore;
    let cols_lengths_offset = output_cursor |> position |> Int64.of_int in
    Stateful_buffers.set_int64_be offsets_bytes 0 cols_offset;
    Stateful_buffers.set_int64_be offsets_bytes 8 cols_lengths_offset;
    output_cursor
    |> write cols_lengths_bf.position cols_lengths_bf.buffer
    |> write 16 offsets_bytes |> ignore

  let serialize (buffer_size : int) (logcols : (string * Column.col) list)
      (_output_cursor : OC.t) =
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
