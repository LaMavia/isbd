let middleware handler req =
  Lwt.catch (fun () -> handler req)
  @@ fun e ->
  let exc = Printexc.to_string e
  and stack = Printexc.get_backtrace () in
  let problem = Models.MultipleProblemsError.{ error = exc; context = Some stack } in
  [%yojson_of: Models.MultipleProblemsError.t] [ problem ]
  |> Yojson.Safe.to_string
  |> Dream.json ~status:`Bad_Request
;;
