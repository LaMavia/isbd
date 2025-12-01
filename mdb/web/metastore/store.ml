open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type raw = { tables : TableData.t list } [@@deriving yojson]

type t =
  { config : Core.Config.t
  ; mutable tables : (Core.Uuid.t, TableData.t) Hashtbl.t
  }

let yojson_of_t ms =
  yojson_of_raw { tables = Hashtbl.to_seq_values ms.tables |> List.of_seq }
;;

let empty config = { tables = Hashtbl.create 0; config }

let load (config : Core.Config.t) =
  let ic = open_in config.metastore_path in
  let content = really_input_string ic (in_channel_length ic) in
  match content with
  | json when String.trim json = "" -> empty config
  | json ->
    Printf.eprintf "json=«%s»\n" json;
    flush_all ();
    let r = Yojson.Safe.from_string json |> raw_of_yojson in
    { tables =
        List.length r.tables
        |> Hashtbl.create ~random:true
        |> List.fold_right
             TableData.(
               fun t u ->
                 Hashtbl.replace u t.id t;
                 u)
             r.tables
    ; config
    }
;;

let save (config : Core.Config.t) (ms : t) =
  yojson_of_t ms |> Yojson.Safe.to_file config.metastore_path
;;

let get_table_path id ms =
  String.concat "" [ ms.config.table_directory; "/"; Core.Uuid.to_string id; ".bin" ]
;;

let create_table id td ms =
  let module C = Lib.Cursor.MMapCursor in
  let module Serializer = Lib.Serializer.Make (C) in
  let file_path = get_table_path id ms in
  let cursor = C.create file_path |> Result.get_ok in
  Serializer.serialize 0 TableData.(td.columns) Seq.empty cursor;
  C.truncate cursor;
  C.close cursor;
  Hashtbl.replace ms.tables id td
;;

let drop_table id ms = Hashtbl.remove ms.tables id
