open Core
open QueryTask
open Models

let handler (req : Dream.request) =
  let tq = Dream.field req Middleware.TaskQueueMiddleware.field |> Option.get
  and ms = Dream.field req Middleware.MetastoreMiddleware.field |> Option.get in
  let ticket = TaskQueue.get_ticket tq in
  let%lwt body = Dream.body req in
  let query = Yojson.Safe.from_string body |> [%of_yojson: ExecuteQueryRequest.t] in
  let td_opt =
    Planner.Validate.get_query_table ms query.query_definition
    |> Metastore.TableData.inc_ref
  in
  let task_id =
    TaskQueue.add_task { request = query; td_opt } ticket QueryStatus.Created tq
  in
  task_id |> [%yojson_of: TaskQueue.id] |> Yojson.Safe.to_string |> Dream.json ~status:`OK
;;
