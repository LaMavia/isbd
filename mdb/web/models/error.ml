open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t = { message : string } [@@deriving yojson]

exception WebError of t

let web_error e = WebError e
