open Middleware
open Core

let handler (req : Dream.request) =
  let tq = Dream.field req TaskQueueMiddleware.field |> Option.get in
  let query_id = Dream.param req "query_id" |> TaskQueue.id_of_string in
  match TaskQueue.peek_result_opt query_id tq with
  | None, None -> Dream.empty `Not_Found
  | r, Some s ->
    let open Models.Query in
    let is_result_available = Option.is_some r in
    { is_result_available = Some is_result_available
    ; query_id = TaskQueue.string_of_id query_id
    ; query_definition =
        (match r with
         | Some (Ok qr) -> Some qr.query_definition
         | _ -> None)
    ; status = s
    }
    |> [%yojson_of: t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`OK
  | _ -> Dream.empty `Internal_Server_Error
;;
