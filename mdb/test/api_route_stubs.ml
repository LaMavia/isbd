let test_stubs =
  [ `GET, "/tables"
  ; `GET, "/table/id"
  ; `DELETE, "/table/id"
  ; `PUT, "/table"
  ; `GET, "/queries"
  ; `GET, "/query/id"
  ; `POST, "/query"
  ; `GET, "/result/id"
  ; `GET, "/error/id"
  ; `GET, "/system/info"
  ]
  |> List.map (fun (method_, path) ->
    Alcotest.test_case path `Quick
    @@ fun () ->
    let res = Dream.test Web.app (Dream.request ~method_ ~target:path "") in
    Alcotest.(check int)
      "Not imeplemented"
      (Dream.status res |> Dream.status_to_int)
      (Dream.status_to_int `Not_Implemented))
;;

let suite = [ "endpoint stubs", test_stubs ]
let () = Alcotest.run __FILE__ suite
