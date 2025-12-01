let handler (req : Dream.request) =
  let ms = Dream.field req Middleware.MetastoreMiddleware.field |> Option.get in
  let%lwt body = Dream.body req in
  let schema = Yojson.Safe.from_string body |> [%of_yojson: Models.TableSchema.t] in
  let id = Core.Uuid.v4 () in
  let td =
    Metastore.TableData.
      { id
      ; name = schema.name
      ; columns = Array.map Models.Column.to_lib schema.columns
      ; relative_path = Printf.sprintf "%s.bin" (Core.Uuid.to_string id)
      }
  in
  Metastore.Store.create_table id td ms;
  id |> [%yojson_of: Core.Uuid.t] |> Yojson.Safe.to_string |> Dream.json ~status:`OK
;;
