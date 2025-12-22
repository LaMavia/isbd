(* let () = Dream.run @@ Dream.logger @@ Web.Main.app *)
let () =
  let open Web.Planner.Eval in
  let open Lib in
  Memtrace.trace_if_requested ();
  let n = 1_000_000L in
  let stream : Data.data_record Seq.t =
    Seq.init (Int64.to_int n) (fun _ ->
      [| `DataInt (Random.int64 n); `DataInt (Random.int64 n) |])
    |> Seq.take (Int64.to_int n)
    |> Seq.once
  in
  stream
  |> with_generic_external_sort
       ~n_workers:10
       ~cols:[| "col1", `ColInt; "col2", `ColInt |]
       ~cmp:compare
       ~est_size:Data.approx_record_size
       ~max_group_size:(Web.Metastore.Const.buffer_size / 8)
       ~k_way_threshold:40
     (*      @@ Seq.iter ignore *)
     (* ;; *)
     @@ Seq.iteri (fun i r -> Printf.eprintf "%d: %s\n" i (Data.string_of_record r))
;;
