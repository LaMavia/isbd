open Ppx_yojson_conv_lib.Yojson_conv.Primitives
open Middleware
open Core

let handler (req : Dream.request) =
  let tq = Dream.field req TaskQueueMiddleware.field |> Option.get in
  let query_id = Dream.param req "query_id" |> TaskQueue.id_of_string in
  match TaskQueue.peek_result_opt query_id tq with
  | None, None ->
    let open Models.Error in
    { message =
        Printf.sprintf "Query with id=%s not found" (TaskQueue.string_of_id query_id)
    }
    |> [%yojson_of: t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`Not_Found
  | r, Some status ->
    let open Models.Query in
    { is_result_available =
        Option.map
          (function
            | Error _ -> false
            | Ok r -> Option.is_some QueryTask.(r.result_id))
          r
    ; query_id = TaskQueue.string_of_id query_id
    ; query_definition =
        (match r with
         | Some (Ok qr) -> Some qr.query_definition
         | _ -> None)
    ; status
    }
    |> [%yojson_of: t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`OK
  | r, s ->
    (r, s)
    |> [%yojson_of: QueryTask.result_ option * QueryTask.status option]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`Internal_Server_Error
;;
