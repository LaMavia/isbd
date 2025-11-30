open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { name : string
  ; id : Core.Uuid.t
  ; relative_path : string
  ; columns : Models.Column.t array
  }
[@@deriving yojson]
