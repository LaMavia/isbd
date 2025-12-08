open Ppx_yojson_conv_lib.Yojson_conv.Primitives
open Middleware
open Utils
open Core

let get_handler (req : Dream.request) =
  let open Models.QueryStatus in
  let open QueryTask in
  let tq =
    Dream.field req TaskQueueMiddleware.field
    |> Unwrap.option ~message:"Task queue is undefined"
  and ms =
    Dream.field req MetastoreMiddleware.field
    |> Unwrap.option ~message:"Metastore is undefined"
  in
  let task_id = Dream.param req "query_id" |> TaskQueue.id_of_string in
  match TaskQueue.peek_result_opt task_id tq with
  | Some (Ok { result_id = Some res_id; _ }), Some Completed ->
    (match Metastore.Store.lookup_result_by_id res_id ms with
     | None -> Dream.empty `Internal_Server_Error
     | Some td ->
       let open Models.QueryResult in
       Metastore.Store.with_read_result td ms
       @@ fun data ->
       let data = Array.of_seq data in
       let row_count = Array.length data
       and column_count = Array.length td.columns in
       let data = Matrix.transpose row_count column_count data in
       let columns =
         Array.mapi
           (fun col_i col_data ->
              Models.ColumnValue.of_lib_array (snd td.columns.(col_i)) col_data)
           data
       in
       { columns; row_count = Some row_count }
       |> [%yojson_of: t]
       |> Yojson.Safe.to_string
       |> Dream.json ~status:`OK)
  | r, s ->
    Dream.log
      "r=%s, s=%s\n"
      ([%yojson_of: QueryTask.result_ option] r |> Yojson.Safe.to_string)
      ([%yojson_of: QueryTask.status option] s |> Yojson.Safe.to_string);
    Dream.empty `Not_Found
;;

let routes =
  let open Dream in
  [ get "/result/:query_id" get_handler ]
;;
