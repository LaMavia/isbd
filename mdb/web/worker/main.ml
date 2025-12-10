open Middleware
open Core
open Models.ExecuteQueryRequest
open Models.QueryStatus
open QueryTask

let process_select ms tq task_id query_def query =
  let open Models.SelectQuery in
  let open Utils.Let.Opt in
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

let make_copy_selector td query =
  let open Models.CopyQuery in
  let open Metastore.TableData in
  let message = "Destination column error" in
  match query.destination_columns with
  | None ->
    Logger.log `Debug "[%s] Returning id selector" __FUNCTION__;
    Ok Fun.id
  | Some mappings ->
    if Array.length mappings != Array.length td.columns
    then Error { message; details = "Invalid number of destination columns" }
    else (
      match Utils.Validation.are_unique (Array.to_list mappings) with
      | Error col ->
        Error { message; details = Printf.sprintf "Duplicate destination column %s" col }
      | Ok _ ->
        let table_column_indices =
          td.columns
          |> Array.to_seqi
          |> Seq.map (fun (i, c) -> fst c, i)
          |> Hashtbl.of_seq
        in
        Hashtbl.to_seq table_column_indices
        |> Seq.iter (fun (k, i) -> Logger.log `Debug "k=%s, i=%d" k i);
        flush_all ();
        (match
           Array.find_opt (Fun.negate (Hashtbl.mem table_column_indices)) mappings
         with
         | Some col ->
           Error { message; details = Printf.sprintf "Invalid destination column %s" col }
         | None ->
           let permutation = Array.map (Hashtbl.find table_column_indices) mappings in
           Logger.log
             `Debug
             "permutation: %s"
             (String.concat ", " (Array.to_list permutation |> List.map string_of_int));
           flush_all ();
           Result.ok
           @@ fun csv_row ->
           Logger.log `Debug "called selector";
           flush_all ();
           let r = Array.map (Array.get csv_row) permutation in
           Array.iteri (fun i c -> Logger.log `Debug "select result: %d -> %s" i c) r;
           r))
;;

let process_copy ms tq task_id query =
  let open Models.CopyQuery in
  let open QueryTask in
  let table_name = query.destination_table_name in
  let csv_path = Metastore.Store.resolve_data_path query.source_filepath ms in
  let error_handler e = TaskQueue.add_result task_id (Error e) Failed tq in
  if not (Sys.is_regular_file csv_path)
  then
    error_handler
      { message = "Failed to find input file"
      ; details =
          Printf.sprintf "Couldn't find file «%s» at «%s»" query.source_filepath csv_path
      }
  else (
    match Metastore.Store.lookup_table_by_name table_name ms with
    | None ->
      error_handler
        { message = "Undefined table name"
        ; details = Printf.sprintf "Couldn't find table «%s»" table_name
        }
    | Some td ->
      TaskQueue.set_status task_id Running tq;
      let column_types = td.columns |> Array.map snd in
      (match make_copy_selector td query with
       | Error e -> error_handler e
       | Ok selector ->
         Logger.log `Debug "Built selector";
         flush_all ();
         CsvParser.read_csv ~has_header:query.does_csv_contain_header csv_path
         |> CsvParser.parse_channel ~selector ~columns:column_types
         |> Metastore.Store.append_table td ms;
         Logger.log `Debug "Appended to table";
         flush_all ();
         TaskQueue.add_result
           task_id
           (Ok { query_definition = QD_CopyQuery query; result_id = None })
           Completed
           tq))
;;

let main (ms : Metastore.Store.t) (tq : TaskQueueMiddleware.t) () =
  Logger.log `Info "Spawned";
  flush_all ();
  while true do
    let task_id, task = TaskQueue.pop_task Planning tq in
    let query_def = task.request.query_definition in
    Logger.log `Info "Popped task: %s" (TaskQueue.string_of_id task_id);
    (try
       match query_def with
       | QD_SelectQuery query -> process_select ms tq task_id query_def query
       | QD_CopyQuery query -> process_copy ms tq task_id query
     with
     | QueryTaskError e -> TaskQueue.add_result task_id (Error e) Failed tq
     | e ->
       let e_str = Printexc.to_string e in
       let e_name = Printexc.exn_slot_name e in
       TaskQueue.add_result
         task_id
         (Error { message = Printf.sprintf "Unexpected error %s" e_name; details = e_str })
         Failed
         tq);
    Logger.log `Info "DONE";
    flush_all ()
  done
;;
