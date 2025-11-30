let routes =
  let open Dream in
  let open Utils.Placeholder in
  [ get "/queries" unimplemented
  ; scope "/query" [] [ get "/:queryId" unimplemented; post "" unimplemented ]
  ]
;;
