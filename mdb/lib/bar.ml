let rate ~title ?(width = 5) () =
  let open Progress.Line in
  list [ const title; parens (sum ~width () ++ const "/? done") ]
;;
