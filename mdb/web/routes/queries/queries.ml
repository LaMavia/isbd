let routes =
  let open Dream in
  let open Utils.Placeholder in
  [ get "/queries" Queries_get.handler
  ; scope "/query" [] [ get "/:queryId" unimplemented; post "" Query.Post.handler ]
  ]
;;
