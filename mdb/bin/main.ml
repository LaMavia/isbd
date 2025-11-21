open Lib
(* open Bigarray *)

type mode =
  | Serialize
  | Deserialize

let () =
  let module C = Cursor.MMapCursor in
  let module TestSerializer = Serializer.Make (C) in
  let module TestDeserializer = Deserializer.Make (C) in
  let usage_msg = "mdb [-s] [-d] <file> ..." in
  let mode = ref Serialize
  and input_files = ref [] in
  let anon_fun filename = input_files := filename :: !input_files in
  let make_set_mode m = Arg.Unit (fun () -> (mode := m) |> ignore) in
  let speclist =
    [ "-s", make_set_mode Serialize, "serialize"
    ; "-d", make_set_mode Deserialize, "deserialize"
    ]
  in
  Memtrace.trace_if_requested ();
  Arg.parse speclist anon_fun usage_msg;
  let path = List.hd !input_files in
  let cursor = C.create path |> Result.get_ok in
  let n = 10_000 in
  match !mode with
  | Serialize ->
    let cols =
      [| "col_int_1", `ColInt
         (* ; "col_int_2", `ColInt *)
         (* ; "col_vchar_1", `ColString *)
         (* ; "col_vchar_2", `ColString *)
      |]
    in
    TestSerializer.serialize
      1_000_000
      cols
      (Seq.init n (fun i ->
         Data.Types.
           [| DataInt (Int64.of_int (i + 1))
              (* ; DataInt (Int64.mul 432435L (Int64.of_int (i + 1))) *)
              (* ; DataVarchar (Printf.sprintf "Hello %d" i) *)
              (* ; DataVarchar (List.init (i + 1) (Fun.const ":3") |> String.concat "|") *)
           |]))
      cursor;
    TestSerializer.write_columns cols cursor;
    C.truncate cursor;
    C.close cursor
  | Deserialize ->
    let columns, chunks_length = TestDeserializer.read_columns cursor in
    columns
    |> Array.iter (fun (s, t) ->
      Printf.eprintf
        "%s\t%s\n"
        s
        (match t with
         | `ColInt -> "INT"
         | `ColString -> "VCHAR"));
    Printf.eprintf "\n";
    TestDeserializer.deserialize cursor columns chunks_length
    |> Test_func.test_func columns
    |> Array.iter2
         (fun col u ->
            Printf.eprintf "%s: " (fst col);
            match u with
            | Either.Left avg -> Printf.eprintf "%.02f\n" avg
            | Either.Right dict ->
              Printf.eprintf "{\n";
              Hashtbl.iter (fun c cnt -> Printf.eprintf "\t'%c':%d\n" c cnt) dict;
              Printf.eprintf "\t}\n")
         columns
;;
