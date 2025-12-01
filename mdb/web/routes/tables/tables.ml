let routes =
  let open Dream in
  let open Utils.Placeholder in
  [ get "/tables" Tables_get.handler
  ; scope
      "/table"
      []
      [ get "/:tableId" unimplemented
      ; delete "/:tableId" unimplemented
      ; put "" Table.Put.handler
      ]
  ]
;;
