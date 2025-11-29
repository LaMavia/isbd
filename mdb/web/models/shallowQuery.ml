type t =
  { query_id : QueryId.t [@key "queryId"]
  ; status : QueryStatus.t
  }
[@@deriving yojson]
