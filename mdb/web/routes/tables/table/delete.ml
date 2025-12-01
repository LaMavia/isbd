let handler (req : Dream.request) =
  let ms = Dream.field req Middleware.MetastoreMiddleware.field |> Option.get in
  let table_id = Dream.param req "table_id" |> Core.Uuid.of_string in
  Metastore.Store.drop_table table_id ms;
  Dream.empty `OK
;;
