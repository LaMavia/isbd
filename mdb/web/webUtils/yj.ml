let alt (fs : (string * (Yojson.Safe.t -> 'b)) list) (v : Yojson.Safe.t) : 'b =
  let rec aux fs e =
    match fs with
    | [] -> raise e
    | (_l, f) :: fs' ->
      (try
         (* Printf.eprintf "[%s] Trying %s on %s\n" __FUNCTION__ l (Yojson.Safe.to_string v); *)
         let r = f v in
         (* Printf.eprintf "DONE\n\n"; *)
         r
       with
       (* | Ppx_yojson_conv_lib.Yojson_conv.Of_yojson_error (exc, json) as e' -> *)
       (*   Printf.eprintf *)
       (*     "FAILED: %s on %s\n\n" *)
       (*     (Printexc.to_string_default exc) *)
       (*     (Yojson.Safe.to_string json); *)
       (*   aux fs' e' *)
       | e' ->
         (* Printf.eprintf "FAILED: %s\n\n" (Printexc.to_string_default e'); *)
         aux fs' e')
  in
  (* Printf.eprintf "[%s] v=%s\n" __FUNCTION__ (Yojson.Safe.to_string v); *)
  aux fs (Invalid_argument "Empty alternative")
;;

let yojson_of_result yojson_of_r yojson_of_e = function
  | Error e -> `Assoc [ "type", `String "error"; "value", yojson_of_e e ]
  | Ok r -> `Assoc [ "type", `String "ok"; "value", yojson_of_r r ]
;;

let result_of_yojson r_of_yojson e_of_yojson = function
  | `Assoc [ ("type", `String "error"); ("value", e) ] -> Error (e_of_yojson e)
  | `Assoc [ ("type", `String "ok"); ("value", r) ] -> Ok (r_of_yojson r)
  | json ->
    Yojson.json_error
    @@ Printf.sprintf
         "Expected ({ \"type\": \"error\", \"value\": _ }|{ \"type\": \"ok\", \"value\": \
          _ }) but got %s instead"
         (Yojson.Safe.to_string json)
;;

module I64 = struct
  open Yojson

  type t = int64

  (** https://stackoverflow.com/questions/22246577/yojson-to-parse-int64-ocaml *)
  let t_of_yojson (json : Yojson.Safe.t) =
    match json with
    | `Intlit i -> Int64.of_string i
    | `Int i -> Int64.of_int i
    | d ->
      json_error
        (Printf.sprintf "Expected an int64 but got %s instead" (Safe.to_string d))
  ;;

  let yojson_of_t i = `Intlit (Int64.to_string i)
end
