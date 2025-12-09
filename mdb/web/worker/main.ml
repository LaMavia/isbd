open Middleware
open Core
open Models.ExecuteQueryRequest
open Models.QueryStatus

let process_select ms tq task_id query_def query =
  let open Models.SelectQuery in
  let open Utils.Let.Opt in
  let open QueryTask in
  match query.table_name with
  | None ->
    Logger.log
      `Error
      "Table %s not found"
      (Option.fold ~none:"None" ~some:Fun.id query.table_name);
    TaskQueue.add_result
      task_id
      (Error
         { message = "Table name not provided"
         ; details = "Expected tableName to be a string but got null instead"
         })
      Failed
      tq
  | Some table_name ->
    (match Metastore.Store.lookup_table_by_name table_name ms with
     | None ->
       TaskQueue.add_result
         task_id
         (Error
            { message = "Undefined table name"
            ; details = Printf.sprintf "Couldn't find table «%s»" table_name
            })
         Failed
         tq
     | Some td ->
       let- table_lock = Hashtbl.find_opt ms.locks td.id in
       let id = TaskQueue.uuid_of_id task_id in
       Mutex.protect table_lock (fun () ->
         Metastore.Store.with_read_table td ms
         @@ fun data ->
         TaskQueue.set_status task_id Running tq;
         let data = List.of_seq data in
         List.to_seq data
         |> Metastore.Store.create_result
              Metastore.TableData.
                { name = Printf.sprintf "%s-result" (TaskQueue.string_of_id task_id)
                ; id
                ; columns = td.columns
                }
              ms);
       TaskQueue.add_result
         task_id
         (Ok { query_definition = query_def; result_id = Some id })
         Completed
         tq)
;;

let process_copy ms tq task_id query =
  let open Models.CopyQuery in
  let open QueryTask in
  let table_name = query.destination_table_name in
  let csv_path = Metastore.Store.resolve_data_path query.source_filepath ms in
  if not (Sys.is_regular_file csv_path)
  then
    TaskQueue.add_result
      task_id
      (Error
         { message = "Failed to find input file"
         ; details =
             Printf.sprintf
               "Couldn't find file «%s» at «%s»"
               query.source_filepath
               csv_path
         })
      Failed
      tq
  else (
    match Metastore.Store.lookup_table_by_name table_name ms with
    | None ->
      TaskQueue.add_result
        task_id
        (Error
           { message = "Undefined table name"
           ; details = Printf.sprintf "Couldn't find table «%s»" table_name
           })
        Failed
        tq
    | Some td ->
      TaskQueue.set_status task_id Running tq;
      let column_types = td.columns |> Array.to_list |> List.map snd in
      CsvParser.read_csv ~has_header:query.does_csv_contain_header csv_path
      |> CsvParser.parse_channel ~selector:Fun.id ~columns:column_types
      |> Metastore.Store.write_table td ms;
      TaskQueue.add_result
        task_id
        (Ok { query_definition = QD_CopyQuery query; result_id = None })
        Completed
        tq)
;;

let main (ms : Metastore.Store.t) (tq : TaskQueueMiddleware.t) () =
  Logger.log `Info "Spawned";
  while true do
    let task_id, task = TaskQueue.pop_task Planning tq in
    let query_def = task.request.query_definition in
    Logger.log `Info "Popped task: %s" (TaskQueue.string_of_id task_id);
    (match query_def with
     | QD_SelectQuery query -> process_select ms tq task_id query_def query
     | QD_CopyQuery query -> process_copy ms tq task_id query);
    Logger.log `Info "DONE";
    flush_all ()
  done
;;
