open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { name : string
  ; type_ : LogicalColumnType.t [@key "type"]
  }
[@@deriving yojson]
