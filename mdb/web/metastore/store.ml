open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type raw = { tables : TableData.t list } [@@deriving yojson]

type t =
  { config : Core.Config.t
  ; mutable id_tables : (Core.Uuid.t, TableData.t) Hashtbl.t
  ; mutable name_tables : (string, TableData.t) Hashtbl.t
  }

module Internal = struct
  module Cursor = Lib.Cursor.MMapCursor
  module Serializer = Lib.Serializer.Make (Cursor)
  module Deserializer = Lib.Deserializer.Make (Cursor)

  let set_table td ms =
    let open TableData in
    Hashtbl.replace ms.id_tables td.id td;
    Hashtbl.replace ms.name_tables td.name td
  ;;

  let remove_table id name ms =
    Hashtbl.remove ms.id_tables id;
    Hashtbl.remove ms.name_tables name
  ;;
end

open Internal

let yojson_of_t ms =
  yojson_of_raw { tables = Hashtbl.to_seq_values ms.id_tables |> List.of_seq }
;;

let empty config =
  { id_tables = Hashtbl.create 0; name_tables = Hashtbl.create 0; config }
;;

let load (config : Core.Config.t) =
  let ic = open_in config.metastore_path in
  let content = really_input_string ic (in_channel_length ic) in
  match content with
  | json when String.trim json = "" -> empty config
  | json ->
    let r = Yojson.Safe.from_string json |> raw_of_yojson in
    let tables_len = List.length r.tables in
    let ms =
      { id_tables = Hashtbl.create ~random:true tables_len
      ; name_tables = Hashtbl.create ~random:true tables_len
      ; config
      }
    in
    List.iter
      TableData.(
        fun t ->
          Hashtbl.replace ms.id_tables t.id t;
          Hashtbl.replace ms.name_tables t.name t)
      r.tables;
    ms
;;

let save (config : Core.Config.t) (ms : t) =
  yojson_of_t ms |> Yojson.Safe.to_file config.metastore_path
;;

let get_table_path id ms =
  Printf.sprintf "%s/%s.bin" ms.config.table_directory (Core.Uuid.to_string id)
;;

let get_result_path id ms =
  Printf.sprintf "%s/%s.bin" ms.config.result_directory (Core.Uuid.to_string id)
;;

let create_table id td ms =
  let file_path = get_table_path id ms in
  let cursor = Cursor.create file_path |> Result.get_ok in
  Serializer.serialize 0 TableData.(td.columns) Seq.empty cursor;
  Cursor.truncate cursor;
  Cursor.close cursor;
  set_table td ms
;;

let drop_table id ms =
  let td = Hashtbl.find ms.id_tables id in
  let file_path = get_table_path id ms in
  Sys.remove file_path;
  remove_table td.id td.name ms
;;

let read_table td ms =
  let open TableData in
  let path = get_table_path td.id ms in
  let cursor = Cursor.create path |> Result.get_ok in
  Deserializer.deserialize cursor |> snd
;;

let save_result id (td : TableData.t) ms (stream : Lib.Data.data_record Seq.t) =
  let open Internal in
  let result_path = get_result_path id ms in
  let cursor = Cursor.create result_path |> Result.get_ok in
  Serializer.serialize Const.buffer_size td.columns stream cursor
;;

let lookup_table_by_id id ms = Hashtbl.find_opt ms.id_tables id
let lookup_table_by_name name ms = Hashtbl.find_opt ms.name_tables name
