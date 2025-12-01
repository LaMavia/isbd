open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { row_count : int option [@yojson.option] [@key "rowCount"]
  ; columns : ColumnValue.t array [@default [||]]
  }
[@@deriving yojson]
