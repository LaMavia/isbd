open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { interface_version : string option [@yojson.option] [@key "interfaceVersion"]
  ; version : string
  ; author : string option [@yojson.option]
  }
[@@deriving yojson]
