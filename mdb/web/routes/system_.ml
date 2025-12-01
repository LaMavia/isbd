let routes =
  let open Dream in
  [ get "/system/info" (fun _ ->
      let open Models.SystemInformation in
      { author = Some "Zuzanna Surowiec"
      ; version = "0.0.1"
      ; interface_version = Some "1.0.0"
      }
      |> [%yojson_of: t]
      |> Yojson.Safe.to_string
      |> json ~status:`OK)
  ]
;;
