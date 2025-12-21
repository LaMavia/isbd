open Ppx_yojson_conv_lib.Yojson_conv.Primitives
open Core

type raw = { tables : TableData.t list } [@@deriving yojson]

type t =
  { config : Config.t
  ; id_tables : (Uuid.t, TableData.t) Hashtbl.t
  ; name_tables : (string, TableData.t) Hashtbl.t
  ; id_results : (Uuid.t, TableData.t) Hashtbl.t
  ; locks : (Uuid.t, Mutex.t) Hashtbl.t
  ; store_lock : Mutex.t
  }

module Internal = struct
  module Cursor = Lib.Cursor.MMapCursor
  module Serializer = Lib.Serializer.Make (Cursor)
  module Deserializer = Lib.Deserializer.Make (Cursor)

  let set_table td ms =
    let open TableData in
    Hashtbl.replace ms.id_tables td.id td;
    Hashtbl.replace ms.name_tables td.name td;
    Hashtbl.replace ms.locks td.id (Mutex.create ())
  ;;

  let remove_table id name ms =
    Hashtbl.remove ms.id_tables id;
    Hashtbl.remove ms.name_tables name;
    Hashtbl.remove ms.locks id
  ;;

  let set_result td ms =
    let open TableData in
    Hashtbl.replace ms.id_results td.id td;
    Hashtbl.replace ms.locks td.id (Mutex.create ())
  ;;

  let remove_result id ms = Hashtbl.remove ms.id_results id

  let with_read ~resolver td ms f =
    let open TableData in
    let cursors =
      List.map
        (fun file_id -> Cursor.create (resolver td.id file_id ms) |> Result.get_ok)
        td.files
    in
    let res =
      cursors
      |> List.map (fun cursor -> Deserializer.deserialize cursor |> snd)
      |> List.to_seq
      |> Seq.concat
      |> f
    in
    List.iter Cursor.close cursors;
    res
  ;;
end

open Internal

let yojson_of_t ms =
  yojson_of_raw { tables = Hashtbl.to_seq_values ms.id_tables |> List.of_seq }
;;

let empty config =
  { id_tables = Hashtbl.create ~random:true 0
  ; name_tables = Hashtbl.create ~random:true 0
  ; id_results = Hashtbl.create ~random:true 0
  ; locks = Hashtbl.create ~random:true 0
  ; store_lock = Mutex.create ()
  ; config
  }
;;

let load (config : Config.t) =
  if not (Sys.file_exists config.metastore_path)
  then empty config
  else (
    let ic = open_in config.metastore_path in
    Fun.protect ~finally:(fun () -> close_in_noerr ic)
    @@ fun () ->
    let content = really_input_string ic (in_channel_length ic) in
    match content with
    | json when String.trim json = "" -> empty config
    | json ->
      let r = Yojson.Safe.from_string json |> raw_of_yojson in
      let tables_len = List.length r.tables in
      let ms =
        { id_tables = Hashtbl.create ~random:true tables_len
        ; name_tables = Hashtbl.create ~random:true tables_len
        ; id_results = Hashtbl.create ~random:true 0
        ; locks = Hashtbl.create ~random:true tables_len
        ; store_lock = Mutex.create ()
        ; config
        }
      in
      List.iter
        TableData.(
          fun t ->
            Hashtbl.replace ms.id_tables t.id t;
            Hashtbl.replace ms.name_tables t.name t;
            Hashtbl.replace ms.locks t.id (Mutex.create ()))
        r.tables;
      ms)
;;

let save (ms : t) =
  let ms_json = yojson_of_t ms |> Yojson.Safe.to_string in
  let ms_fd =
    Unix.openfile ms.config.metastore_path [ O_WRONLY; O_CREAT; O_TRUNC ] 0o640
  in
  Fun.protect ~finally:(fun () -> Unix.close ms_fd)
  @@ fun () ->
  Unix.single_write_substring ms_fd ms_json 0 (String.length ms_json) |> ignore
;;

let resolve_table_directory tid ms =
  Printf.sprintf "%s/%s" ms.config.table_directory (Uuid.to_string tid)
;;

let resolve_result_directory rid ms =
  Printf.sprintf "%s/%s" ms.config.result_directory (Uuid.to_string rid)
;;

let resolve_table_path tid sid ms =
  Printf.sprintf "%s/%s.bin" (resolve_table_directory tid ms) (Uuid.to_string sid)
;;

let resolve_result_path rid sid ms =
  Printf.sprintf "%s/%s.bin" (resolve_result_directory rid ms) (Uuid.to_string sid)
;;

let resolve_data_path relative_path ms =
  Printf.sprintf "%s/%s" ms.config.data_directory relative_path
;;

let create_table _id td ms =
  Mutex.protect ms.store_lock
  @@ fun () ->
  Sys.mkdir (resolve_table_directory TableData.(td.id) ms) 0o777;
  set_table td ms
;;

let create_result td ms stream =
  let open TableData in
  Sys.mkdir (resolve_result_directory td.id ms) 0o777;
  let file_id = Uuid.v4 () in
  let file_path = resolve_result_path td.id file_id ms in
  let cursor = Cursor.create file_path |> Result.get_ok in
  Serializer.serialize Const.buffer_size td.columns stream cursor;
  Cursor.truncate cursor;
  Cursor.close cursor;
  Mutex.protect ms.store_lock
  @@ fun () ->
  td.files <- file_id :: td.files;
  set_result td ms
;;

let drop_table id ms =
  let open Utils.Let.Opt in
  Mutex.protect ms.store_lock (fun () ->
    let- table_lock = Hashtbl.find_opt ms.locks id in
    Mutex.protect table_lock
    @@ fun () ->
    let- td = Hashtbl.find_opt ms.id_tables id in
    let dir_path = resolve_table_directory id ms in
    WebUtils.File.rm_rec dir_path;
    remove_table td.id td.name ms)
;;

let drop_result id ms =
  let open Utils.Let.Opt in
  Mutex.protect ms.store_lock
  @@ fun () ->
  let- result_lock = Hashtbl.find_opt ms.locks id in
  Mutex.protect result_lock
  @@ fun () ->
  let- td = Hashtbl.find_opt ms.id_results id in
  let dir_path = resolve_result_directory td.id ms in
  WebUtils.File.rm_rec dir_path;
  remove_result td.id ms
;;

let drop_all_results ms =
  ms.id_results
  |> Hashtbl.to_seq_values
  |> Seq.iter (fun td ->
    let open TableData in
    resolve_result_directory td.id ms |> WebUtils.File.rm_rec;
    Hashtbl.remove ms.locks td.id);
  Hashtbl.reset ms.id_results
;;

let with_read_table td ms f = with_read ~resolver:resolve_table_path td ms f
let with_read_result td ms f = with_read ~resolver:resolve_result_path td ms f
let lookup_table_by_id id ms = Hashtbl.find_opt ms.id_tables id
let lookup_table_by_name name ms = Hashtbl.find_opt ms.name_tables name
let lookup_result_by_id id ms = Hashtbl.find_opt ms.id_results id

let append_table td ms stream =
  let open TableData in
  let new_file_id = Uuid.v4 () in
  let new_path = resolve_table_path td.id new_file_id ms in
  let cursor = Cursor.create new_path |> Result.get_ok in
  Serializer.serialize Const.buffer_size td.columns stream cursor;
  Cursor.truncate cursor;
  Cursor.close cursor;
  let output_lock = Hashtbl.find ms.locks td.id in
  Mutex.protect output_lock @@ fun () -> td.files <- new_file_id :: td.files
;;

(** unprotected write, used for sorting, doesn't close the cursor*)
let write path columns stream =
  let cursor = Cursor.create path |> Result.get_ok in
  Serializer.serialize ~encode:true (Const.buffer_size / 8) columns stream cursor;
  Cursor.truncate cursor;
  cursor
;;

let read_cursor cursor =
  Cursor.seek 0 cursor |> Deserializer.deserialize ~decode:true |> snd
;;
