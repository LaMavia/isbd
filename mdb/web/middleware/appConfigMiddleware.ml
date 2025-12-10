let field : Core.Config.t Dream.field = Dream.new_field ~name:"app_config" ()

let middleware config handler req =
  Dream.set_field req field config;
  handler req
;;

