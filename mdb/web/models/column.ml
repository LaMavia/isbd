open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { name : string
  ; type_ : LogicalColumnType.t [@key "type"]
  }
[@@deriving yojson]

let of_lib ((n, t) : Lib.Column.t) : t = { name = n; type_ = LogicalColumnType.of_lib t }
let to_lib (t : t) : Lib.Column.t = t.name, LogicalColumnType.to_lib t.type_
