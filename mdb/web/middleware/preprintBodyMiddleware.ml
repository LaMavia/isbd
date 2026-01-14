let pretty body = body |> Yojson.Safe.from_string |> Yojson.Safe.pretty_to_string

let middleware handler req =
  let%lwt body = Dream.body req in
  (try Dream.log "@@>>>>>>>>>REQUEST: %s%!" (pretty body) with
   | _ -> ());
  let%lwt res = handler req in
  let%lwt res_body = Dream.body res in
  (try Dream.log "@@<<<<<<<<<RESPONSE: %s%!" (pretty res_body) with
   | _ -> ());
  Lwt.return res
;;
