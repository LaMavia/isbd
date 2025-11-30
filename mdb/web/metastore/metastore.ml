open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type raw = { tables : TableData.t list } [@@deriving yojson]
type t = { tables : (Core.Uuid.t, TableData.t) Hashtbl.t }

let t_of_yojson yojson =
  let r = raw_of_yojson yojson in
  { tables =
      List.length r.tables
      |> Hashtbl.create ~random:true
      |> List.fold_right
           TableData.(
             fun t u ->
               Hashtbl.replace u t.id t;
               u)
           r.tables
  }
;;

let yojson_of_t ms =
  yojson_of_raw { tables = Hashtbl.to_seq_values ms.tables |> List.of_seq }
;;

let empty () = { tables = Hashtbl.create 0 }

let load (config : Core.Config.t) =
  let ic = open_in config.metastore_path in
  let content = really_input_string ic (in_channel_length ic) in
  match content with
  | "" -> empty ()
  | json -> Yojson.Safe.from_string json |> t_of_yojson
;;

let save (config : Core.Config.t) (ms : t) =
  yojson_of_t ms |> Yojson.Safe.to_file config.metastore_path
;;
