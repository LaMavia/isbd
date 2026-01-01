open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t = QueryResult.t list [@@deriving yojson]
