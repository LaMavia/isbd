open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { column_index : int [@key "columnIndex"]
  ; ascending : bool
  }
[@@deriving yojson]
