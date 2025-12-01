let routes =
  let open Dream in
  let open Utils.Placeholder in
  [ get "/tables" Tables_get.handler
  ; scope
      "/table"
      []
      [ get "/:table_id" unimplemented
      ; delete "/:table_id" Table.Delete.handler
      ; put "" Table.Put.handler
      ]
  ]
;;
