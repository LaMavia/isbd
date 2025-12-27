let () =
  Alcotest.run
    __FILE__
    [ ( "k way merge"
      , [ Alcotest.test_case "simple merge" `Quick (fun () ->
            let res =
              Web.Planner.Eval.k_way_merge Int.compare List.[| to_seq [ 2; 4; 8; 9 ] |]
              |> List.of_seq
            in
            Alcotest.(check (list int)) "Eq" [ 1; 2; 3; 4; 5; 6; 7; 8; 9 ] res)
        ] )
    ]
;;
