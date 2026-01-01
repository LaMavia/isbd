open! Api_tests.Common

let () =
  let mdb_handler = MdbServer.start () in
  Unix.sleepf 2.;
  Api_tests.[ System_info.test ]
  |> List.map (fun t -> t mdb_handler)
  |> Alcotest_lwt.run __FILE__
  |> Lwt_main.run
;;
