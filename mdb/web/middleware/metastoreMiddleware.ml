let field : Metastore.Store.t Dream.field = Dream.new_field ~name:"metastore" ()

let metastore_update_field : bool Dream.field =
  Dream.new_field ~name:"update_metastore" ~show_value:string_of_bool ()
;;

let mark_dirty req = Dream.set_field req metastore_update_field true

let middleware ms handler req =
  Dream.set_field req field ms;
  Lwt.finalize
    (fun () -> handler req)
    (fun () ->
       let should_save =
         Dream.field req metastore_update_field |> Option.value ~default:false
       in
       if should_save then Metastore.Store.save ms;
       Lwt.return ())
;;
