let routes =
  let open Dream in
  let open Utils.Placeholder in
  [ get "/tables" unimplemented
  ; scope
      "/table"
      []
      [ get "/:tableId" unimplemented
      ; delete "/:tableId" unimplemented
      ; put "" unimplemented
      ]
  ]
;;
