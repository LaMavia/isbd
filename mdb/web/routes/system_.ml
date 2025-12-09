let routes =
  let open Dream in
  [ get "/system/info" (fun req ->
      let open Models.SystemInformation in
      let open Middleware.UptimeMiddleware in
      let uptime =
        Dream.field req field
        |> Utils.Unwrap.option ~message:"Expected uptime middleware to be set"
      in
      { author = "Zuzanna Surowiec"
      ; version = "0.0.1"
      ; interface_version = Some "1.0.1"
      ; uptime
      }
      |> [%yojson_of: t]
      |> Yojson.Safe.to_string
      |> json ~status:`OK)
  ]
;;
