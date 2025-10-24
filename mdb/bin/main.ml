(* open Opium *)
(**)
(* type person = { name : string; age : int } [@@deriving yojson] *)
(**)
(* let print_person_handler req = *)
(*   let name = Router.param req "name" in *)
(*   let age = Router.param req "age" |> int_of_string in *)
(*   let person = { name; age } |> person_to_yojson in *)
(*   Lwt.return (Response.of_json person) *)
(**)
(* let update_person_handler req = *)
(*   let open Lwt.Syntax in *)
(*   let+ json = Request.to_json_exn req in *)
(*   let person = person_of_yojson json |> Result.get_ok in *)
(*   Logs.info (fun m -> m "Received person: %s" person.name); *)
(*   Response.of_json (`Assoc [ ("message", `String "Person saved") ]) *)
(**)
(* let streaming_handler req = *)
(*   let length = Body.length req.Request.body in *)
(*   let content = Body.to_stream req.Request.body in *)
(*   let body = Lwt_stream.map String.uppercase_ascii content in *)
(*   Response.make ~body:(Body.of_stream ?length body) () |> Lwt.return *)
(**)
(* let print_param_handler req = *)
(*   Printf.sprintf "Hello, %s\n" (Router.param req "name") *)
(*   |> Response.of_plain_text |> Lwt.return *)
(**)
(* let () = *)
(*   Printf.printf "Hello\n"; *)
(*   flush_all (); *)
(*   App.empty *)
(*   |> App.post "/hello/stream" streaming_handler *)
(*   |> App.get "/hello/:name" print_param_handler *)
(*   |> App.get "/person/:name/:age" print_person_handler *)
(*   |> App.patch "/person" update_person_handler *)
(*   |> App.port 8000 |> App.run_command *)

open Lib.Schema.Type

let page_size = 16 * 1024

let () =
  Array.iter (fun x -> Printf.printf "%s\n" x) Sys.argv;
  flush_all ();
  let fd = Unix.openfile Sys.argv.(1) Unix.[ O_RDWR; O_APPEND ] 0o640 in
  Unix.write fd (Bytes.make page_size '\000') 0 page_size |> print_int;
  flush_all ();
  let a = Unix.map_file fd Bigarray.Char Bigarray.C_layout true [| -1 |] in
  let p = { age = 505435435; name = "Zuzanna"; surname = "Surowiec" } in
  PersonSig.deserialize a p 0 |> ignore;
  match PersonSig.serialize a 0 with
  | Result.Ok (p', _) ->
      Printf.printf "\nperson { age=%d; name=%s; surname=%s }" p'.age p'.name
        p'.surname
  | Result.Error e -> print_string e.reason
