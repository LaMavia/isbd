let interface = Sys.getenv_opt "HOSTNAME" |> Option.value ~default:"0.0.0.0"
let port = Sys.getenv_opt "PORT" |> Option.fold ~none:80 ~some:int_of_string
let () = Dream.run ~interface ~port @@ Dream.logger @@ Web.Main.app ()
