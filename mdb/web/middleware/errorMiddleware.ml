open Models.MultipleProblemsError

let res_of_exc ?(problems = []) e =
  let exc = Printexc.to_string e
  and stack = Printexc.get_backtrace () in
  let problem = { error = exc; context = Some stack } in
  [%yojson_of: t] { problems = problem :: problems }
  |> Yojson.Safe.to_string
  |> Dream.json ~status:`Bad_Request
;;

let middleware handler req =
  Lwt.catch (fun () -> handler req)
  @@ function
  | Ppx_yojson_conv_lib.Yojson_conv.Of_yojson_error (exc, json) ->
    res_of_exc
      ~problems:[ { error = "json error"; context = Some (Yojson.Safe.to_string json) } ]
      exc
  | e -> res_of_exc e
;;
