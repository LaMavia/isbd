let routes =
  let open Dream in
  [ get "/queries" Queries_get.handler
  ; scope "/query" [] [ get "/:query_id" Query.Get.handler; post "" Query.Post.handler ]
  ]
;;
