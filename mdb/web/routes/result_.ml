open Middleware
open Utils
open Core

let get_handler (req : Dream.request) =
  let open Models.QueryStatus in
  let open Models.GetQueryResultRequest in
  let open QueryTask in
  let tq =
    Dream.field req TaskQueueMiddleware.field
    |> Unwrap.option ~message:"Task queue is undefined"
  and ms =
    Dream.field req MetastoreMiddleware.field
    |> Unwrap.option ~message:"Metastore is undefined"
  in
  let task_id = Dream.param req "query_id" |> TaskQueue.id_of_string in
  let%lwt body = Dream.body req in
  let { flush_result; row_limit } =
    match body with
    | "" -> { flush_result = false; row_limit = None }
    | body ->
      body |> Yojson.Safe.from_string |> [%of_yojson: Models.GetQueryResultRequest.t]
  in
  match TaskQueue.peek_result_opt task_id tq with
  | Some (Ok { result_id = Some res_id; _ }), Some Completed ->
    (match Metastore.Store.lookup_result_by_id res_id ms with
     | None ->
       let open Models.Error in
       { message =
           "Unexpected error: task queue returned completed task, but metastore returned \
            none"
       }
       |> [%yojson_of: t]
       |> Yojson.Safe.to_string
       |> Dream.json ~status:`Bad_Request
     | Some td ->
       let open Models.QueryResult in
       let response =
         Metastore.Store.with_read_result td ms (fun data ->
           Dream.stream ~status:`OK ~close:false
           @@ fun stream ->
           let%lwt () = Dream.write stream "[" in
           let is_first = ref true in
           let%lwt () =
             data
             |> (match row_limit with
               | None -> Fun.id
               | Some limit -> Seq.take limit)
             |> Planner.Eval.group_eph_seq
                  Lib.Data.approx_record_size
                  Metastore.Const.buffer_size
             |> Seq.map Array.of_seq
             |> Lwt_seq.of_seq
             |> Lwt_seq.iter_s (fun chunk ->
               let row_count = Array.length chunk
               and column_count = Array.length td.columns in
               let data = Matrix.transpose row_count column_count chunk in
               let columns =
                 Array.mapi
                   (fun col_i col_data ->
                      Models.ColumnValue.of_lib_array (snd td.columns.(col_i)) col_data)
                   data
               in
               let%lwt () =
                 if !is_first
                 then (
                   is_first := false;
                   Lwt.return_unit)
                 else Dream.write stream ","
               in
               { columns; row_count = Some row_count }
               |> [%yojson_of: Models.QueryResult.t]
               |> Yojson.Safe.to_string
               |> Dream.write stream)
           in
           let%lwt () = Dream.write stream "]" in
           Dream.close stream)
       in
       if flush_result
       then (
         Dream.log "Flushing result %s" (Uuid.to_string res_id);
         Metastore.Store.drop_result res_id ms;
         TaskQueue.pop_result_opt task_id tq |> ignore);
       response)
  | _ ->
    let open Models.Error in
    { message =
        Printf.sprintf "Result with id=%s not found" (TaskQueue.string_of_id task_id)
    }
    |> [%yojson_of: t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`Not_Found
;;

let routes =
  let open Dream in
  [ get "/result/:query_id" get_handler ]
;;
