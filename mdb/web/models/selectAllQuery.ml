open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { table_name : string [@key "tableName"]
  ; limit_clause : LimitExpression.t option [@key "limitClause"] [@yojson.option]
  }
[@@deriving yojson]
