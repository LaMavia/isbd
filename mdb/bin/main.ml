(* let () = Dream.run @@ Dream.logger @@ Web.Main.app *)
let () =
  let open Web.Planner.Eval in
  let open Lib in
  let n = 5_000_000L in
  let stream : Data.data_record Seq.t =
    Seq.ints 0
    |> Seq.map (fun _ -> [| `DataInt (Random.int64 n) |])
    |> Seq.take (Int64.to_int n)
  in
  stream
  |> with_external_sort
       ~cols:[| "col1", `ColInt |]
       ~cmp:(fun a b -> compare a.(0) b.(0))
       ~est_size:Data.approx_record_size
       ~max_group_size:Web.Metastore.Const.buffer_size
     @@ Seq.iter (fun x -> ignore x)
;;
