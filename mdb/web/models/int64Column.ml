(* open Ppx_yojson_conv_lib.Yojson_conv.Primitives *)
open Yojson

type t = int64

(** https://stackoverflow.com/questions/22246577/yojson-to-parse-int64-ocaml *)
let t_of_yojson (json : Yojson.Safe.t) =
  match json with
  | `Intlit i -> Int64.of_string i
  | `Int i -> Int64.of_int i
  | d ->
    json_error (Printf.sprintf "Expected an int64 but got %s instead" (Safe.to_string d))
;;

let yojson_of_t i = `Intlit (Int64.to_string i)
