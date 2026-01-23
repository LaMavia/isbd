open Ppx_yojson_conv_lib.Yojson_conv.Primitives
open Core
open Middleware

let shallow_table_of_table_data (td : Metastore.TableData.t) =
  Models.ShallowTable.{ name = td.name; table_id = Core.Uuid.to_string td.id }
;;

let handler (req : Dream.request) =
  let ms = Dream.field req MetastoreMiddleware.field |> Option.get
  and tq = Dream.field req TaskQueueMiddleware.field |> Option.get in
  let ticket = TaskQueue.get_ticket tq in
  TaskQueue.with_ticket tq ticket
  @@ fun () ->
  let tables =
    ms.id_tables
    |> Hashtbl.to_seq_values
    |> Seq.map shallow_table_of_table_data
    |> List.of_seq
  in
  [%yojson_of: Models.ShallowTable.t list] tables
  |> Yojson.Safe.to_string
  |> Dream.json ~status:`OK
;;
