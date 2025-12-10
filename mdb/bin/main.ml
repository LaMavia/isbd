let () = Dream.run ~interface:"0.0.0.0" ~port:80 @@ Dream.logger @@ Web.Main.app
