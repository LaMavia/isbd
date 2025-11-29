open Ppx_yojson_conv_lib.Yojson_conv.Primitives

(* TODO: Add queryDefinition *)
type t = { description : string option [@yojson.option] } [@@deriving yojson]
