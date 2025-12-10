open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { row_limit : int option [@yojson.option] [@key "rowLimit"]
  ; flush_result : bool [@default false] [@key "flushResult"]
  }
[@@deriving yojson]
