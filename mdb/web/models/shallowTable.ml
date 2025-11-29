open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { table_id : TableId.t [@key "tableId"]
  ; name : string
  }
[@@deriving yojson]
