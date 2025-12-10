open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { interface_version : string option [@yojson.option] [@key "interfaceVersion"]
  ; version : string
  ; author : string
  ; uptime : WebUtils.Yj.I64.t
  }
[@@deriving yojson]
