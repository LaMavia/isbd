open Ppx_yojson_conv_lib.Yojson_conv.Primitives

(* TODO: add queryDefinition *)
type t =
  { query_id : QueryId.t [@key "queryId"]
  ; status : QueryStatus.t
  ; isResultAvailable : bool option
  }
[@@deriving yojson]
