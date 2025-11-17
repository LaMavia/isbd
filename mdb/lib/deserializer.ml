module Make (IC : Cursor.CursorInterface) = struct
  let read_columns (input_cursor : IC.t) =
    let open IC in
    let offsets_len = 16 and input_len = len input_cursor in
    let offset_bytes =
      input_cursor |> seek (input_len - offsets_len) |> read offsets_len
    in
    let cols_offset =
      Stateful_buffers.get_int64_be offset_bytes 0 |> Int64.to_int
    and cols_lengths_offset =
      Stateful_buffers.get_int64_be offset_bytes 8 |> Int64.to_int
    in
    let cols_lens = cols_lengths_offset - cols_offset
    and cols_lengths_lens = input_len - cols_lengths_offset - offsets_len in
    let cols_bytes = input_cursor |> seek cols_offset |> read cols_lens in
    let cols_lengths_bytes = input_cursor |> read cols_lengths_lens in
    let bfs = Stateful_buffers.of_list [ cols_bytes; cols_lengths_bytes ] in
    Column.Deserializers.ColumnInfoDeserializer.decode_fragments bfs 0
      [| cols_lens; cols_lengths_lens |]
      0;
    let cols_bf = Stateful_buffers.get_buffer bfs 0
    and cols_lengths_bf = Stateful_buffers.get_buffer bfs 1 in
    cols_bf.position <- 0;
    cols_lengths_bf.position <- 0;
    Column.Deserializers.ColumnInfoDeserializer.deserialize_seq bfs 0
    |> List.of_seq
end
