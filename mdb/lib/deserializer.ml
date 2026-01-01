module Make (IC : Cursor.CursorInterface) = struct
  open IC
  open Stateful_buffers
  open Bigarray

  let read_columns ?(decode = true) (input_cursor : IC.t) =
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
    let@ bfs = of_list [ cols_bytes; cols_lengths_bytes ] in
    if decode
    then
      Column.Columns.ColumnInfoColumn.decode_fragments
        bfs
        0
        [| cols_lens; cols_lengths_lens |]
        0;
    let cols_bf = get_buffer bfs 0
    and cols_lengths_bf = get_buffer bfs 1 in
    cols_bf.position <- 0;
    cols_lengths_bf.position <- 0;
    let columns = Column.Columns.ColumnInfoColumn.deserialize_seq bfs 0 |> Array.of_seq in
    columns, cols_offset
  ;;

  let physlen_of_logcol = function
    | _, `ColVarchar -> Column.VarcharLogCol.physical_length
    | _, `ColInt -> Column.IntLogCol.physical_length
  ;;

  let deserialize ?(decode = true) ?column_filter:column_filter_opt (input_cursor : IC.t) =
    let raw_logcols, chunks_len = read_columns ~decode input_cursor in
    let column_filter =
      match column_filter_opt with
      | Some cf -> cf
      | None -> Array.init (Array.length raw_logcols) Fun.id
    in
    let filtered_logcols =
      Array.mapi
        (fun i logcol -> if Array.mem i column_filter then Some logcol else None)
        raw_logcols
    in
    let resulting_logcols = Array.map (Array.unsafe_get raw_logcols) column_filter in
    let max_fraglens_len = 2 * Const.max_uint_len * Array.length raw_logcols
    and phys_lens = raw_logcols |> Array.map physlen_of_logcol
    and deserializers =
      raw_logcols
      |> Array.map (function
        | _, `ColInt -> Column.IntLogCol.deserialize_iter
        | _, `ColVarchar -> Column.VarcharLogCol.deserialize_iter)
    and decoders =
      raw_logcols
      |> Array.map (function
        | _, `ColInt -> Column.IntLogCol.decode_fragments
        | _, `ColVarchar -> Column.VarcharLogCol.decode_fragments)
    in
    let raw_physcol_idx_of_raw_logcol_idx =
      Array.to_seq phys_lens |> Seq.scan ( + ) 0 |> Array.of_seq
    in
    let total_physcols =
      Array.fold_left (fun u i -> u + Array.unsafe_get phys_lens i) 0 column_filter
    in
    let raw_logcol_of_raw_physcol =
      Array.to_seqi raw_physcol_idx_of_raw_logcol_idx
      |> Seq.take (Array.length raw_logcols)
      |> Seq.concat_map (fun (li, _) -> Seq.init phys_lens.(li) (Fun.const li))
      |> Array.of_seq
    in
    let suggested_buffer_size =
      get_int64_be (input_cursor |> seek 0 |> read 8) 0 |> Int64.to_int
    in
    let buffer_size = LZ4.compress_bound suggested_buffer_size in
    let parsed_record_seq_dispenser = ref (Fun.const None)
    and fraglen_bfs =
      Stateful_buffers.create ~n:1 ~len:max_fraglens_len ~actual_length:max_fraglens_len
    (* TODO: tu też zmodyfikować do selekcji kolumn *)
    and bfs =
      Stateful_buffers.create
        ~n:total_physcols
        ~len:suggested_buffer_size
        ~actual_length:buffer_size
    in
    (* Printf.eprintf *)
    (*   "[%s] allocating \n  %d×%d fragment bfs,\n  1×%d fraglen bfs\n%!" *)
    (*   __FUNCTION__ *)
    (*   total_physcols *)
    (*   buffer_size *)
    (*   max_fraglens_len; *)
    let rec give_record () =
      try
        match !parsed_record_seq_dispenser () with
        | Option.None when IC.position input_cursor >= chunks_len ->
          free fraglen_bfs;
          free bfs;
          Option.None
        | Option.Some _ as row_opt -> row_opt
        | Option.None ->
          let fraglen_a = Stateful_buffers.get_buffer fraglen_bfs 0 in
          let fraglens_len =
            (Stateful_buffers.get_int64_be (IC.read 8 input_cursor) 0 |> Int64.to_int) - 8
          and prev_fraglen_a_length = fraglen_a.length in
          Array1.(
            blit (IC.read fraglens_len input_cursor) (sub fraglen_a.buffer 0 fraglens_len));
          fraglen_a.position <- 0;
          fraglen_a.length <- fraglens_len;
          if decode
          then
            Column.Columns.IntColumn.decode_fragments fraglen_bfs 0 [| fraglens_len |] 0;
          (* load each column into bfs *)
          (* TODO: tu zmodyfikować do selekcji kolumn *)
          (* INFO: length(flens) = length(column_filter) *)
          let flens =
            let bi = ref 0
            and i = ref 0 in
            Column.Columns.IntColumn.deserialize_seq fraglen_bfs 0
            |> Seq.filter_map (fun flen ->
              let flen = Int64.to_int flen in
              let li = raw_logcol_of_raw_physcol.(!i) in
              if Array.mem li column_filter
              then (
                let b = Stateful_buffers.get_buffer bfs !bi in
                Array1.(blit (IC.read flen input_cursor) (sub b.buffer 0 flen));
                b.length <- flen;
                bi := !bi + 1;
                i := !i + 1;
                Some flen)
              else (
                IC.move flen input_cursor |> ignore;
                i := !i + 1;
                None))
            |> Array.of_seq
          in
          fraglen_a.length <- prev_fraglen_a_length;
          if decode
          then
            Array.fold_left
              (fun (i, ci, fi) logcol_opt ->
                 match logcol_opt with
                 | Some _ ->
                   decoders.(i) bfs fi flens fi;
                   i + 1, ci + 1, fi + phys_lens.(ci)
                 | None -> i + 1, ci, fi)
              (0, 0, 0)
              filtered_logcols
            |> ignore;
          Array.iter (fun b -> b.position <- 0) bfs;
          let n_cols = Array.length resulting_logcols in
          let _, column_dispensers =
            Array.fold_left_map
              (fun (i, ci, bi) logcol_opt ->
                 match logcol_opt with
                 | Some _ ->
                   ( (i + 1, ci + 1, bi + phys_lens.(ci))
                   , deserializers.(i) bfs bi |> Seq.to_dispenser )
                 | None -> (i + 1, ci, bi), Fun.const None)
              (0, 0, 0)
              filtered_logcols
          in
          let aux () =
            try
              Some
                (Array.init n_cols (fun ci ->
                   let coli = column_filter.(ci) in
                   column_dispensers.(coli) () |> Option.get))
            with
            | Invalid_argument _ -> None
          in
          parsed_record_seq_dispenser := aux;
          empty fraglen_bfs;
          empty bfs;
          give_record ()
      with
      | e ->
        free bfs;
        free fraglen_bfs;
        raise_notrace e
    in
    resulting_logcols, Seq.of_dispenser give_record
  ;;
end
