let interface = Sys.getenv_opt "MDB_HOSTNAME" |> Option.value ~default:"0.0.0.0"
let port = Sys.getenv_opt "MDB_PORT" |> Option.fold ~none:80 ~some:int_of_string
let () = Dream.run ~interface ~port @@ Dream.logger @@ Web.Main.app ()
