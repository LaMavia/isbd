open Core

type t = (QueryTask.task, QueryTask.result_, QueryTask.status) TaskQueue.t

let field : t Dream.field =
  Dream.new_field
    ~name:"task_queue"
    ~show_value:
      (TaskQueue.show
         ~status:
           (Some (fun s -> Models.QueryStatus.yojson_of_t s |> Yojson.Safe.to_string)))
    ()
;;

let middleware (q : t) (handler : Dream.handler) req =
  Dream.set_field req field q;
  handler req
;;
