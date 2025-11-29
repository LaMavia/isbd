open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t = { table_name : string option [@key "tableName"] [@yojson.option] }
[@@deriving yojson]
