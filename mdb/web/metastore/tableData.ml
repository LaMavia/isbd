open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type raw =
  { name : string
  ; id : Core.Uuid.t
  ; relative_path : string
  ; columns : Models.Column.t array
  }
[@@deriving yojson]

type t =
  { name : string
  ; id : Core.Uuid.t
  ; relative_path : string
  ; columns : Lib.Column.t array
  }

let t_of_yojson (json : Yojson.Safe.t) : t =
  let r = raw_of_yojson json in
  { name = r.name
  ; id = r.id
  ; relative_path = r.relative_path
  ; columns = Array.map Models.Column.to_lib r.columns
  }
;;

let yojson_of_t td =
  yojson_of_raw
    { name = td.name
    ; id = td.id
    ; relative_path = td.relative_path
    ; columns = Array.map Models.Column.of_lib td.columns
    }
;;
