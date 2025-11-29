open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { description : string option [@yojson.option]
  ; query_definition : QueryDefinition.t option [@yojson.option] [@key "queryDefinition"]
  }
[@@deriving yojson]
