(* let () = Dream.run @@ Dream.logger @@ Web.Main.app *)
let () =
  (*   Seq.ints 0 *)
  (*   |> Seq.take 50 *)
  (*   |> Seq.once *)
  (*   |> Web.Planner.Eval.group_eph_seq (fun a b -> abs (a - b) < 10) *)
  (*   |> Seq.iteri (fun i g -> *)
  (*     Printf.eprintf "group %d: " i; *)
  (*     Seq.iter (Printf.eprintf "%d ") g; *)
  (*     Printf.eprintf "\n") *)
  (* ;; *)
  let open Web.Planner.Eval in
  let open Lib in
  let n = 6_000_000L in
  let stream : Data.data_record Seq.t =
    Seq.ints 0
    |> Seq.map (fun _ -> [| `DataInt (Random.int64 n) |])
    |> Seq.take (Int64.to_int n)
    |> Seq.once
  in
  stream
  |> with_external_sort
       ~cols:[| "col1", `ColInt |]
       ~cmp:(fun a b -> compare a.(0) b.(0))
       ~est_size:Data.approx_record_size
       ~max_group_size:(2 * Web.Metastore.Const.buffer_size)
     @@ Seq.iter ignore
;;
