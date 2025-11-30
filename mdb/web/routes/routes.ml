let router =
  Dream.router
  @@ List.concat
       [ Tables.routes; Query.routes; Result_.routes; Error_.routes; System_.routes ]
;;
