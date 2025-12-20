let routes =
  let open Dream in
  let open Middleware in
  let open Utils in
  let open Core in
  [ (get "/error/:query_id"
     @@ fun req ->
     let tq =
       Dream.field req TaskQueueMiddleware.field
       |> Unwrap.option ~message:"Task queue not set"
     and task_id = Dream.param req "query_id" |> TaskQueue.id_of_string in
     match TaskQueue.peek_result_opt task_id tq with
     | Some (Error errors), Some Models.QueryStatus.Failed ->
       let open Models.MultipleProblemsError in
       errors |> [%yojson_of: t] |> Yojson.Safe.to_string |> Dream.json ~status:`OK
     | _ ->
       let open Models.Error in
       { message =
           Printf.sprintf
             "Failed to find error of query with id=%s"
             (TaskQueue.string_of_id task_id)
       }
       |> [%yojson_of: t]
       |> Yojson.Safe.to_string
       |> Dream.json ~status:`Not_Found)
  ]
;;
