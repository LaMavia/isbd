module Make (OC : Cursor.CursorInterface) = struct
  module OutChunk = Chunk.Make (OC)

  let write_columns (logcols : (string * Column.col) array)
      (output_cursor : OC.t) =
    let open OC in
    let cols_offset = output_cursor |> position |> Int64.of_int in
    let max_total_cols_len_approx, max_total_cols_lengths_len =
      Array.fold_right
        (fun (s, _) (u, ul) -> (u + String.length s + 1, ul + 9))
        logcols (0, 0)
    in
    let max_total_cols_len = LZ4.compress_bound max_total_cols_len_approx in
    let cols_bytes = Stateful_buffers.create_bytes max_total_cols_len
    and cols_lengths_bytes =
      Stateful_buffers.create_bytes max_total_cols_lengths_len
    and offsets_bytes = Stateful_buffers.create_bytes 16 in
    let bfs =
      Stateful_buffers.of_list [ cols_bytes; cols_lengths_bytes; offsets_bytes ]
    in
    (* Serialize each column *)
    logcols
    |> Array.iter (fun logcol ->
           Column.Serializers.ColumnInfoSerializer.serialize logcol bfs 0);
    Column.Deserializers.ColumnInfoDeserializer.encode_fragments bfs 0;
    let cols_bf = Stateful_buffers.get_buffer bfs 0 in
    let cols_lengths_bf = Stateful_buffers.get_buffer bfs 1 in
    (* Dump column bytes *)
    output_cursor |> write cols_bf.position cols_bf.buffer |> ignore;
    let cols_lengths_offset = output_cursor |> position |> Int64.of_int in
    Stateful_buffers.set_int64_be offsets_bytes 0 cols_offset;
    Stateful_buffers.set_int64_be offsets_bytes 8 cols_lengths_offset;
    output_cursor
    |> write cols_lengths_bf.position cols_lengths_bf.buffer
    |> write 16 offsets_bytes |> ignore

  let serialize (buffer_size_suggestion : int)
      (logcols : (string * Column.col) array)
      (record_stream : Data.data_record Seq.t) (output_cursor : OC.t) =
    let buffer_size = LZ4.compress_bound buffer_size_suggestion in
    let phys_lens =
      logcols
      |> Array.map (function
           | _, `ColString -> Column.VarcharLogCol.physical_length
           | _, `ColInt -> Column.IntLogCol.physical_length)
    and serializers =
      logcols
      |> Array.map (function
           | _, `ColInt -> Column.IntLogCol.serialize_mut
           | _, `ColString -> Column.VarcharLogCol.serialize_mut)
    and encoders =
      logcols
      |> Array.map (function
           | _, `ColInt -> Column.IntLogCol.encode_fragments
           | _, `ColString -> Column.VarcharLogCol.encode_fragments)
    in
    let total_physcols = Array.fold_right ( + ) phys_lens 0 in
    let record_bfs =
      Stateful_buffers.create ~n:total_physcols ~len:buffer_size_suggestion
        ~actual_length:buffer_size
    and chunk_bfs =
      Stateful_buffers.create ~n:total_physcols ~len:buffer_size_suggestion
        ~actual_length:buffer_size
    in
    let rec can_append_record_to_chunk () =
      Array.for_all2
        Stateful_buffers.(
          fun b ch_b -> b.position < ch_b.length - ch_b.position)
        record_bfs chunk_bfs
    and append_record_to_chunk () =
      if not (can_append_record_to_chunk ()) then (
        encode_fragments ();
        dump_buffers ());
      Stateful_buffers.blit record_bfs chunk_bfs;
      Stateful_buffers.empty record_bfs
    and dump_buffers () =
      let open Stateful_buffers in
      Stateful_buffers.print_buffers "Chunk fragments" chunk_bfs;
      let len = 9 * Array.length chunk_bfs in
      let lengths_bfs = Stateful_buffers.create ~n:1 ~len ~actual_length:len in
      Array.iter
        (fun b ->
          Column.Serializers.UIntSerializer.serialize (Int64.of_int b.position)
            lengths_bfs 0)
        chunk_bfs
      |> ignore;
      (* Stateful_buffers.print_buffers "Chunk lengths" lengths_bfs; *)
      let lengths_a = get_buffer lengths_bfs 0 in
      OC.write lengths_a.position lengths_a.buffer output_cursor |> ignore;
      Array.iter
        (fun b -> OC.write b.position b.buffer output_cursor |> ignore)
        chunk_bfs;
      Stateful_buffers.empty chunk_bfs
    and encode_fragments () =
      let encode_column ((i, fi) : int * int) encoder =
        encoder chunk_bfs fi;
        (i + 1, fi + phys_lens.(i))
      in
      Array.fold_left encode_column (0, 0) encoders |> ignore
    in

    let process_record (r : Data.data_record) =
      let process_column ((i, bi) : int * int) (v : Data.Types.t) =
        serializers.(i) v record_bfs bi;
        (i + 1, bi + phys_lens.(i))
      in
      Array.fold_left process_column (0, 0) r |> ignore;
      append_record_to_chunk ()
    in

    Seq.iter process_record record_stream;
    if not (Stateful_buffers.are_empty chunk_bfs) then (
      encode_fragments ();
      dump_buffers ())
end
