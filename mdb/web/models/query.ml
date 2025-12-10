open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { query_id : QueryId.t [@key "queryId"]
  ; status : QueryStatus.t
  ; is_result_available : bool option [@yojson.option] [@key "isResultAvailable"]
  ; query_definition : QueryDefinition.t option [@yojson.option] [@key "queryDefinition"]
  }
[@@deriving yojson]
