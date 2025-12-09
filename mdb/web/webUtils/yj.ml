let alt (fs : ('a -> 'b) list) (v : 'a) : 'b =
  let rec aux fs e =
    match fs with
    | [] -> raise_notrace e
    | f :: fs' ->
      (try f v with
       | e' -> aux fs' e')
  in
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
