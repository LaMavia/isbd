open Middleware
open Core

(* open Models *)
open Models.ExecuteQueryRequest
open Models.QueryStatus
open QueryTask
open! Utils.Ops

let process_select ms tq task_id query_definition query =
  let open Utils.Let.Opt in
  let id = TaskQueue.uuid_of_id task_id in
  let- td_opt, tlock_opt, query, column_types =
    Mutex.protect
      Metastore.Store.(ms.store_lock)
      (fun () ->
         match Planner.Validate.validate_select_query ms query with
         | _, Error problems ->
           TaskQueue.add_result task_id (Error problems) Failed tq;
           None
         | td, Ok (query, column_types) ->
           let query = Planner.Preprocess.preprocess_select_query query in
           let tlock =
             Option.map Metastore.TableData.(fun td -> Hashtbl.find ms.locks td.id) td
           in
           Option.iter Mutex.lock tlock;
           TaskQueue.set_status task_id Running tq;
           List.iteri
             (fun i c ->
                Printf.eprintf
                  "%d: %s, %s\n"
                  i
                  ([%yojson_of: Models.ColumnExpression.t] c |> Yojson.Safe.to_string)
                  (Models.ColumnExpression.string_of_expt_type (List.nth column_types i)))
             query.column_clauses;
           Some (td, tlock, query, column_types))
  in
  try
    Fun.protect
      ~finally:(fun () -> Option.iter Mutex.unlock tlock_opt)
      (fun () ->
         let _x =
           let with_record_stream =
             match td_opt with
             | Some td -> Metastore.Store.with_read_table td ms
             | None -> fun f -> f (List.to_seq [ [||] ])
           in
           with_record_stream (fun data ->
             let open Planner.Eval in
             let eval = make_eval_ce td_opt in
             let result_td =
               Metastore.TableData.
                 { name = Printf.sprintf "%s-result" (TaskQueue.string_of_id task_id)
                 ; id
                 ; columns =
                     column_types
                     |> List.mapi (fun i t ->
                       ( Printf.sprintf "c%d" i
                       , Models.ColumnExpression.lib_of_expr_type_exc t ))
                     |> Array.of_list
                 ; files = []
                 }
             in
             Seq.filter (filter eval query.where_clause) data
             |> Seq.map (map eval query.column_clauses)
             |> Seq.map Array.of_list
             |> with_sort ~ms ~td:result_td ~order_by_clause_opt:query.order_by_clause
                @@ limit ~limit_clause_opt:query.limit_clause
                @> Metastore.Store.create_result result_td ms;
             TaskQueue.add_result
               task_id
               (Ok { query_definition; result_id = Some id })
               Completed
               tq)
         in
         ())
  with
  | QueryTaskError problems -> TaskQueue.add_result task_id (Error problems) Failed tq
;;

let make_copy_selector td query =
  let open Models.CopyQuery in
  let open Metastore.TableData in
  let error = "Destination column error" in
  match query.destination_columns with
  | None -> Ok Fun.id
  | Some mappings ->
    if Array.length mappings <> Array.length td.columns
    then
      Error
        Models.MultipleProblemsError.
          [ { error; context = Some "Invalid number of destination columns" } ]
    else (
      match Utils.Validation.are_unique (Array.to_list mappings) with
      | Error col ->
        Error
          Models.MultipleProblemsError.
            [ { error
              ; context = Some (Printf.sprintf "Duplicate destination column %s" col)
              }
            ]
      | Ok _ ->
        let table_column_indices =
          td.columns
          |> Array.to_seqi
          |> Seq.map (fun (i, c) -> fst c, i)
          |> Hashtbl.of_seq
        in
        (match
           Array.find_opt (Fun.negate (Hashtbl.mem table_column_indices)) mappings
         with
         | Some col ->
           Error
             Models.MultipleProblemsError.
               [ { error
                 ; context = Some (Printf.sprintf "Invalid destination column %s" col)
                 }
               ]
         | None ->
           let permutation = Array.map (Hashtbl.find table_column_indices) mappings in
           Result.ok @@ fun csv_row -> Array.map (Array.get csv_row) permutation))
;;

let process_copy ms tq task_id query =
  let open Models.CopyQuery in
  let open QueryTask in
  let open Models.MultipleProblemsError in
  let table_name = query.destination_table_name in
  let csv_path = Metastore.Store.resolve_data_path query.source_filepath ms in
  let error_handler e = TaskQueue.add_result task_id (Error e) Failed tq in
  if not (Sys.is_regular_file csv_path)
  then
    error_handler
      [ { error = "Failed to find input file"
        ; context =
            Some
              (Printf.sprintf
                 "Couldn't find file «%s» at «%s»"
                 query.source_filepath
                 csv_path)
        }
      ]
  else (
    match Metastore.Store.lookup_table_by_name table_name ms with
    | None ->
      error_handler
        [ { error = "Undefined table name"
          ; context = Some (Printf.sprintf "Couldn't find table «%s»" table_name)
          }
        ]
    | Some td ->
      TaskQueue.set_status task_id Running tq;
      let column_types = td.columns |> Array.map snd in
      (match make_copy_selector td query with
       | Error e -> error_handler e
       | Ok selector ->
         CsvParser.read_csv ~has_header:query.does_csv_contain_header csv_path
         |> CsvParser.parse_channel ~selector ~columns:column_types
         |> Metastore.Store.append_table td ms;
         TaskQueue.add_result
           task_id
           (Ok { query_definition = QD_CopyQuery query; result_id = None })
           Completed
           tq))
;;

let main (ms : Metastore.Store.t) (tq : TaskQueueMiddleware.t) () =
  Logger.log `Info "Spawned";
  flush_all ();
  try
    while true do
      let task_id, task = TaskQueue.pop_task Planning tq in
      let query_def = task.request.query_definition in
      Logger.log `Info "Starting task: %s" (TaskQueue.string_of_id task_id);
      (try
         match query_def with
         | QD_SelectQuery query -> process_select ms tq task_id query_def query
         | QD_CopyQuery query ->
           process_copy ms tq task_id query;
           Mutex.protect ms.store_lock @@ fun () -> Metastore.Store.save ms
       with
       | QueryTaskError e -> TaskQueue.add_result task_id (Error e) Failed tq
       | e ->
         let e_stack = Printexc.get_backtrace () in
         let e_str = Printexc.to_string e in
         (* let e_name = Printexc.exn_slot_name e in *)
         TaskQueue.add_result
           task_id
           (Error
              Models.MultipleProblemsError.
                [ { error = Printf.sprintf "Unexpected error %s" e_str
                  ; context = Some e_stack
                  }
                ])
           Failed
           tq);
      Logger.log `Info "DONE";
      flush_all ()
    done
  with
  | TaskQueue.ShouldStop -> Logger.log `Info "Exiting"
;;
