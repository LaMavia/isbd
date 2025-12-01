open Ppx_yojson_conv_lib.Yojson_conv.Primitives
open Core

let handler (req : Dream.request) =
  let tq = Dream.field req Middleware.TaskQueueMiddleware.field |> Option.get in
  let open Models.ShallowQuery in
  TaskQueue.peek_statuses tq
  |> List.of_seq
  |> List.map (fun (id, s) -> { query_id = TaskQueue.string_of_id id; status = s })
  |> [%yojson_of: t list]
  |> Yojson.Safe.to_string
  |> Dream.json ~status:`OK
;;
