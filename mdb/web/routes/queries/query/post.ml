open Core
open QueryTask
open Models

let handler (req : Dream.request) =
  let tq = Dream.field req Middleware.TaskQueueMiddleware.field |> Option.get in
  let%lwt body = Dream.body req in
  let query = Yojson.Safe.from_string body |> [%of_yojson: ExecuteQueryRequest.t] in
  let task_id = TaskQueue.add_task { request = query } QueryStatus.Created tq in
  task_id |> [%yojson_of: TaskQueue.id] |> Yojson.Safe.to_string |> Dream.json ~status:`OK
;;
