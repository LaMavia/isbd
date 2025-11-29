(* open Ppx_yojson_conv_lib.Yojson_conv.Primitives *)

type t =
  | I64 of Int64Column.t list
  | VC of VarcharColumn.t list

module Internal = struct
  open Yojson.Safe.Util

  let i64_of_yojson json = I64 (convert_each Int64Column.t_of_yojson json)
  let vc_of_yojson json = VC (convert_each VarcharColumn.t_of_yojson json)
end

let t_of_yojson (json : Yojson.Safe.t) =
  Utils.Yj.alt Internal.[ i64_of_yojson; vc_of_yojson ] json
;;

let yojson_of_t (v : t) : Yojson.Safe.t =
  match v with
  | I64 is -> `List (List.map Int64Column.yojson_of_t is)
  | VC ss -> `List (List.map VarcharColumn.yojson_of_t ss)
;;
