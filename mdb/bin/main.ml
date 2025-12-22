(* let () = Dream.run @@ Dream.logger @@ Web.Main.app *)
let () =
  let open Web.Planner.Eval in
  let open Lib in
  Memtrace.trace_if_requested ();
  let n = 10_000_000L in
  let stream : Data.data_record Seq.t =
    Seq.init (Int64.to_int n) (fun _ -> [| `DataInt (Random.int64 n) |])
    |> Seq.take (Int64.to_int n)
    |> Seq.once
  in
  stream
  |> with_parallel_external_sort
       ~n_workers:5
       ~cols:[| "col1", `ColInt |]
       ~cmp:(fun a b -> compare a.(0) b.(0))
       ~est_size:Data.approx_record_size
       ~max_group_size:(Web.Metastore.Const.buffer_size / 8)
     @@ Seq.iter ignore
;;
(* @@ Seq.iteri (fun i r -> *)
(*   Printf.eprintf "%d: %Ld\n" i (r.(0) |> Data.Types.get_int64 |> Result.get_ok)) *)
