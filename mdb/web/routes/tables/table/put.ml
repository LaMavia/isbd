open Middleware

let column_error schema =
  let open Models.TableSchema in
  let seen_columns = Hashtbl.create ~random:true (Array.length schema.columns) in
  Array.fold_left
    Models.Column.(
      fun u { name; _ } ->
        match u with
        | Error _ -> u
        | Ok _ when Hashtbl.mem seen_columns name ->
          Error (Printf.sprintf "Duplicate column %s" name)
        | Ok _ ->
          Hashtbl.replace seen_columns name ();
          Ok ())
    (Ok ())
    schema.columns
;;

let table_name_error schema ms =
  let open Models.TableSchema in
  match Metastore.Store.lookup_table_by_name schema.name ms with
  | Some td ->
    Error
      (Printf.sprintf
         "Table %s already exists under id=%s"
         schema.name
         (Core.Uuid.to_string td.id))
  | None -> Ok ()
;;

let respond_with_error message =
  let open Models.MultipleProblemsError in
  [ { error = "Error validating schema"; context = Some message } ]
  |> [%yojson_of: t]
  |> Yojson.Safe.to_string
  |> Dream.json ~status:`Bad_Request
;;

let handler (req : Dream.request) =
  let ( let$ ) = Utils.Let.Res.handler respond_with_error in
  let ms = Dream.field req MetastoreMiddleware.field |> Option.get in
  let%lwt body = Dream.body req in
  let schema = Yojson.Safe.from_string body |> [%of_yojson: Models.TableSchema.t] in
  let$ () = table_name_error schema ms in
  let$ () = column_error schema in
  let id = Core.Uuid.v4 () in
  let td =
    Metastore.TableData.
      { id; name = schema.name; columns = Array.map Models.Column.to_lib schema.columns }
  in
  Metastore.Store.create_table id td ms;
  MetastoreMiddleware.mark_dirty req;
  id |> [%yojson_of: Core.Uuid.t] |> Yojson.Safe.to_string |> Dream.json ~status:`OK
;;
