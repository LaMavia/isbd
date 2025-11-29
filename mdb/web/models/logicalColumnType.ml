type t =
  | Int64 [@name "INT64"]
  | Varchar [@name "VARCHAR"]
[@@deriving yojson]
