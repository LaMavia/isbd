let serialization_test v expected_v_str to_yojson =
  Alcotest.test_case "serialize" `Quick
  @@ fun () ->
  let v_str = v |> to_yojson |> Yojson.Safe.to_string in
  Alcotest.(check string) "Equal" expected_v_str v_str
;;

let suite =
  [ ( "LogicalColumnType"
    , Web.Models.LogicalColumnType.[ serialization_test Int64 "\"INT64\"" yojson_of_t ] )
  ]
;;

let () = Alcotest.run __FILE__ suite
