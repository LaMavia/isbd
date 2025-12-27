open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type raw =
  { name : string
  ; id : Core.Uuid.t
  ; columns : Models.Column.t array
  ; files : Core.Uuid.t list
  }
[@@deriving yojson]

type t =
  { name : string
  ; id : Core.Uuid.t
  ; columns : Lib.Column.t array
  ; mutable files : Core.Uuid.t list
  }

let t_of_yojson (json : Yojson.Safe.t) : t =
  let r = raw_of_yojson json in
  { name = r.name
  ; id = r.id
  ; columns = Array.map Models.Column.to_lib r.columns
  ; files = r.files
  }
;;

let yojson_of_t td =
  yojson_of_raw
    { name = td.name
    ; id = td.id
    ; columns = Array.map Models.Column.of_lib td.columns
    ; files = td.files
    }
;;

let to_table_schema (td : t) : Models.TableSchema.t =
  Models.TableSchema.
    { name = td.name; columns = Array.map Models.Column.of_lib td.columns }
;;

let find_column_opt td column_name =
  Array.find_opt (fun (cname, _) -> cname = column_name) td.columns
;;
