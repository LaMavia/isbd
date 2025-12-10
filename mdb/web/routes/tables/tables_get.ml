open Ppx_yojson_conv_lib.Yojson_conv.Primitives

let shallow_table_of_table_data (td : Metastore.TableData.t) =
  Models.ShallowTable.{ name = td.name; table_id = Core.Uuid.to_string td.id }
;;

let handler (req : Dream.request) =
  let ms = Dream.field req Middleware.MetastoreMiddleware.field |> Option.get in
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
