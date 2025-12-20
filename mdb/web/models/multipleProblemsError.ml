open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type problem =
  { error : string
  ; context : string option [@yojson.option]
  }
[@@deriving yojson]

type t = problem list [@@deriving yojson]

exception MultipleProblemsError of t

let make e = MultipleProblemsError e

let of_string_result error =
  Result.map_error (fun msg -> [ { error; context = Some msg } ])
;;
