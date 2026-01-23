open Core
open Middleware

let handler (req : Dream.request) =
  let ms = Dream.field req MetastoreMiddleware.field |> Option.get
  and tq = Dream.field req TaskQueueMiddleware.field |> Option.get in
  let ticket = TaskQueue.get_ticket tq in
  let table_id = Dream.param req "table_id" |> Core.Uuid.of_string in
  TaskQueue.with_ticket tq ticket
  @@ fun () ->
  match Metastore.Store.lookup_table_by_id table_id ms with
  | None ->
    let open Models.Error in
    { message = Printf.sprintf "Table %s not found" (Core.Uuid.to_string table_id) }
    |> [%yojson_of: t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`Not_Found
  | Some td ->
    td
    |> Metastore.TableData.to_table_schema
    |> [%yojson_of: Models.TableSchema.t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`OK
;;
