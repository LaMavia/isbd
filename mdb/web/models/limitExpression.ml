open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t = { limit : int } [@@deriving yojson]
