let routes =
  let open Dream in
  [ get "/queries" Queries_get.handler
  ; scope "/query" [] Query.[ get "/:query_id" Get.handler; post "" Post.handler ]
  ]
;;
