open Ppx_yojson_conv_lib.Yojson_conv.Primitives

let table_of_file (f : string) =
  let uuid_length = 36 in
  let table_id = String.sub f 0 uuid_length
  and name = String.sub f uuid_length (String.length f - uuid_length) in
  Models.ShallowTable.{ name; table_id }
;;

let handler (req : Dream.request) =
  let config = Dream.field req Middleware.AppConfigMiddleware.field |> Option.get in
  let tables =
    Sys.readdir config.table_directory
    |> Array.to_list
    |> List.filter (Fun.negate @@ String.starts_with ~prefix:".")
    |> List.map table_of_file
  in
  [%yojson_of: Models.ShallowTable.t list] tables
  |> Yojson.Safe.to_string
  |> Dream.json ~status:`OK
;;
