open Middleware
open Core

let main (ms : Metastore.Store.t) (tq : TaskQueueMiddleware.t) () =
  Logger.log `Info "Spawned";
  while true do
    let open Models.ExecuteQueryRequest in
    let open Models.QueryStatus in
    let task_id, task = TaskQueue.pop_task Running tq in
    let query_def = task.request.query_definition in
    Logger.log `Info "Popped task: %s" (TaskQueue.string_of_id task_id);
    match query_def with
    | QD_SelectQuery query ->
      let open Models.SelectQuery in
      (match query.table_name with
       | None ->
         TaskQueue.add_result
           task_id
           QueryTask.(
             Error
               { message = "Table name not provided"
               ; details = "Expected tableName to be a string but got null instead"
               })
           Failed
           tq
       | Some table_name ->
         (match Metastore.Store.lookup_table_by_name table_name ms with
          | Some td ->
            let result_id = TaskQueue.uuid_of_id task_id in
            Metastore.Store.read_table td ms
            |> Metastore.Store.save_result result_id td ms;
            TaskQueue.add_result
              task_id
              (Result.Ok
                 QueryTask.{ query_definition = query_def; result_id = Some result_id })
              Completed
              tq
          | None ->
            TaskQueue.add_result
              task_id
              (Result.Error
                 QueryTask.
                   { message = "Undefined table name"
                   ; details = Printf.sprintf "Couldn't find table «%s»" table_name
                   })
              Failed
              tq))
    | QD_CopyQuery _ -> ()
  done
;;
