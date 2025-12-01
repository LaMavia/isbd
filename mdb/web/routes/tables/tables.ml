let routes =
  let open Dream in
  [ get "/tables" Tables_get.handler
  ; scope
      "/table"
      []
      [ get "/:table_id" Table.Get.handler
      ; delete "/:table_id" Table.Delete.handler
      ; put "" Table.Put.handler
      ]
  ]
;;
