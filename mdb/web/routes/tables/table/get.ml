let handler (req : Dream.request) =
  let ms = Dream.field req Middleware.MetastoreMiddleware.field |> Option.get in
  let table_id = Dream.param req "table_id" |> Core.Uuid.of_string in
  match Metastore.Store.lookup_table table_id ms with
  | None -> Dream.empty `Not_Found
  | Some td ->
    td
    |> Metastore.TableData.to_table_schema
    |> [%yojson_of: Models.TableSchema.t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`OK
;;
