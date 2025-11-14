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
  let get_columns (input_cursor : IC.t) =
    let open IC in
    let cols_len =
      Bytes.get_int64_be
        (input_cursor |> seek (len input_cursor - 8) |> read 8)
        0
      |> Int64.to_int
    in
    let col_bytes =
      input_cursor |> move (-8) |> move (-cols_len) |> read cols_len
    in
    let bfs = Stateful_buffers.of_list [ col_bytes ] in
    let uint_dispenser = Column.Deserializers.UIntDeserializer.deserialize_dispenser bfs 0 in
    seek 0 input_cursor |> ignore;
    Seq.of_dispenser (fun () ->
        let open Utils.Mopt in
        let+ clen = uint_dispenser () in
        if !pos >= cols_len then Option.none
        else
          let clen = Bytes.get_int64_be col_bytes !pos |> Int64.to_int in
          pos.contents <- !pos + 8;
          let cname = Bytes.sub_string col_bytes !pos clen in
          pos.contents <- !pos + clen;
          let col =
            Bytes.get col_bytes !pos |> function
            | '\001' -> ColInt cname |> Option.some
            | '\002' -> ColString cname |> Option.some
            | _ -> None
          in
          col)
    |> List.of_seq

  let serialize (buffer_size : int) (logcols : c list) (_input_cursor : IC.t)
      (_output_cursor : OC.t) =
    let phys_lens =
      logcols
      |> List.map (function
           | ColString _ -> Column.VarcharLogCol.physical_length
           | ColInt _ -> Column.IntLogCol.physical_length)
    in
    let _bfs =
      let total_physcols = List.fold_right ( + ) phys_lens 0 in
      Stateful_buffers.create ~n:total_physcols ~len:buffer_size
    in
    ()
end
