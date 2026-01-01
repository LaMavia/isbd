open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { column_clauses : ColumnExpression.t list [@key "columnClauses"]
  ; where_clause : ColumnExpression.t option [@key "whereClause"] [@yojson.option]
  ; order_by_clause : OrderByExpression.t array option
        [@key "orderByClause"] [@yojson.option]
  ; limit_clause : LimitExpression.t option [@key "limitClause"] [@yojson.option]
  }
[@@deriving yojson]
