open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type problem =
  { error : string
  ; context : string option [@yojson.option]
  }
[@@deriving yojson]

type t = problem list [@@deriving yojson]
