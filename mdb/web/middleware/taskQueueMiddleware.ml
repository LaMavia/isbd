open Core

type t = (QueryTask.task, QueryTask.result_, QueryTask.status) TaskQueue.t

let field : t Dream.field = Dream.new_field ~name:"task_queue" ()

let middleware (q : t) (handler : Dream.handler) req =
  Dream.set_field req field q;
  handler req
;;
