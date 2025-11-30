let field : Metastore.Store.t Dream.field = Dream.new_field ~name:"metastore" ()

let middleware ms handler req =
  let config = Dream.field req AppConfigMiddleware.field |> Option.get in
  Dream.set_field req field ms;
  Lwt.finalize
    (fun () -> handler req)
    (fun () -> Metastore.Store.save config ms |> Lwt.return)
;;
