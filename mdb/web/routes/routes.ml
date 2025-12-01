let router =
  Dream.router
  @@ List.concat
       [ Tables.routes; Queries.routes; Result_.routes; Error_.routes; System_.routes ]
;;
