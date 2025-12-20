open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { column_clauses : ColumnExpression.t list [@key "columnClauses"]
  ; where_clause : ColumnExpression.t option [@key "whereClause"] [@yojson.option]
  }
[@@deriving yojson]
