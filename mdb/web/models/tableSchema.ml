open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { name : string
  ; columns : Column.t array
  }
[@@deriving yojson]
