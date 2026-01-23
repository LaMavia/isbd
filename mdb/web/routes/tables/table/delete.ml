open Middleware
open Core

let handler (req : Dream.request) =
  let ms = Dream.field req MetastoreMiddleware.field |> Option.get
  and tq = Dream.field req TaskQueueMiddleware.field |> Option.get in
  let ticket = TaskQueue.get_ticket tq in
  let table_id = Dream.param req "table_id" |> Core.Uuid.of_string in
  TaskQueue.with_ticket tq ticket
  @@ fun () ->
  Metastore.Store.drop_table table_id ms;
  MetastoreMiddleware.mark_dirty req;
  Dream.empty `OK
;;
