open Ppx_yojson_conv_lib.Yojson_conv.Primitives
open WebUtils.Yj

type task = { request : Models.ExecuteQueryRequest.t }
type status = Models.QueryStatus.t [@@deriving yojson]

type err =
  { message : string
  ; details : string
  }
[@@deriving yojson]

type res_ =
  { query_definition : Models.QueryDefinition.t
  ; result_id : Uuid.t option
  }
[@@deriving yojson]

type result_ = (res_, err) result [@@deriving yojson]
