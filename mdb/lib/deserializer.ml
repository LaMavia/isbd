module Make (IC : Cursor.CursorInterface) = struct
  open IC
  open Stateful_buffers
  open Bigarray

  let read_columns (input_cursor : IC.t) =
    let offsets_len = 16
    and input_len = len input_cursor in
    let offset_bytes =
      input_cursor |> seek (input_len - offsets_len) |> read offsets_len
    in
    let cols_offset = get_int64_be offset_bytes 0 |> Int64.to_int
    and cols_lengths_offset = get_int64_be offset_bytes 8 |> Int64.to_int in
    let cols_lens = cols_lengths_offset - cols_offset
    and cols_lengths_lens = input_len - cols_lengths_offset - offsets_len in
    let cols_bytes = input_cursor |> seek cols_offset |> read cols_lens in
    let cols_lengths_bytes = input_cursor |> read cols_lengths_lens in
    let bfs = of_list [ cols_bytes; cols_lengths_bytes ] in
    Column.Deserializers.ColumnInfoDeserializer.decode_fragments
      bfs
      0
      [| cols_lens; cols_lengths_lens |]
      0;
    let cols_bf = get_buffer bfs 0
    and cols_lengths_bf = get_buffer bfs 1 in
    cols_bf.position <- 0;
    cols_lengths_bf.position <- 0;
    let columns =
      Column.Deserializers.ColumnInfoDeserializer.deserialize_seq bfs 0 |> Array.of_seq
    in
    columns, cols_offset
  ;;

  let deserialize (input_cursor : IC.t) =
    let logcols, chunks_len = read_columns input_cursor in
    let max_fraglens_len = Const.max_uint_len * Array.length logcols
    and phys_lens =
      logcols
      |> Array.map (function
        | _, `ColVarchar -> Column.VarcharLogCol.physical_length
        | _, `ColInt -> Column.IntLogCol.physical_length)
    and deserializers =
      logcols
      |> Array.map (function
        | _, `ColInt -> Column.IntLogCol.deserialize_iter
        | _, `ColVarchar -> Column.VarcharLogCol.deserialize_iter)
    and decoders =
      logcols
      |> Array.map (function
        | _, `ColInt -> Column.IntLogCol.decode_fragments
        | _, `ColVarchar -> Column.VarcharLogCol.decode_fragments)
    in
    let total_physcols = Array.fold_right ( + ) phys_lens 0 in
    let suggested_buffer_size =
      get_int64_be (input_cursor |> seek 0 |> read 8) 0 |> Int64.to_int
    in
    let buffer_size = LZ4.compress_bound suggested_buffer_size in
    let parsed_record_seq = ref Seq.empty
    and fraglen_bfs =
      Stateful_buffers.create ~n:1 ~len:max_fraglens_len ~actual_length:max_fraglens_len
    and bfs =
      Stateful_buffers.create
        ~n:total_physcols
        ~len:suggested_buffer_size
        ~actual_length:buffer_size
    in
    let rec give_record () =
      match Seq.uncons !parsed_record_seq with
      | Option.None when IC.position input_cursor >= chunks_len -> Option.None
      | Option.Some (h, t) ->
        parsed_record_seq := t;
        Option.Some h
      | Option.None ->
        let fraglen_a = Stateful_buffers.get_buffer fraglen_bfs 0 in
        let fraglens_len =
          (Stateful_buffers.get_int64_be (IC.read 8 input_cursor) 0 |> Int64.to_int) - 8
        and prev_fraglen_a_length = fraglen_a.length in
        Array1.(
          blit (IC.read fraglens_len input_cursor) (sub fraglen_a.buffer 0 fraglens_len));
        fraglen_a.position <- 0;
        fraglen_a.length <- fraglens_len;
        Column.Deserializers.IntDeserializer.decode_fragments
          fraglen_bfs
          0
          [| fraglens_len |]
          0;
        (* load each column into bfs *)
        let flens =
          Column.Deserializers.IntDeserializer.deserialize_seq fraglen_bfs 0
          |> Array.of_seq
          |> Array.mapi (fun i flen ->
            let flen = Int64.to_int flen in
            let b = Stateful_buffers.get_buffer bfs i in
            Array1.(blit (IC.read flen input_cursor) (sub b.buffer 0 flen));
            b.length <- flen;
            flen)
        in
        fraglen_a.length <- prev_fraglen_a_length;
        Array.fold_left
          (fun (i, fi) _ ->
             decoders.(i) bfs fi flens fi;
             i + 1, fi + phys_lens.(i))
          (0, 0)
          logcols
        |> ignore;
        Array.iter (fun b -> b.position <- 0) bfs;
        let n_cols = Array.length logcols in
        let column_vals = Array.init n_cols (Fun.const [||]) in
        Array.fold_left
          (fun (i, bi) _ ->
             column_vals.(i) <- deserializers.(i) bfs bi |> Array.of_seq;
             i + 1, bi + phys_lens.(i))
          (0, 0)
          logcols
        |> ignore;
        let aux =
          let pos = ref 0 in
          fun () ->
            try
              (pos := !pos + 1;
               Array.init n_cols (fun ci -> column_vals.(ci).(!pos - 1)))
              |> Option.some
            with
            | Invalid_argument _ -> Option.None
        in
        parsed_record_seq := Seq.of_dispenser aux;
        empty fraglen_bfs;
        empty bfs;
        give_record ()
    in
    logcols, Seq.of_dispenser give_record
  ;;
end
