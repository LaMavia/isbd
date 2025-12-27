open! Lib

type mode =
  | Serialize
  | Deserialize

let () =
  let t1 = Unix.time () in
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
  let n = 1_000 in
  match !mode with
  | Serialize ->
    let cols =
      [| "col_int_1", `ColInt
       ; "col_int_2", `ColInt
       ; "col_vchar_1", `ColVarchar
       ; "col_vchar_2", `ColVarchar
      |]
    in
    TestSerializer.serialize
      32_000_000
      cols
      (Seq.init n (fun i ->
         [| `DataInt (Int64.of_int (i + 1))
          ; `DataInt (Int64.mul 2L (Int64.of_int (i + 1)))
          ; `DataVarchar "Hello"
          ; `DataVarchar "There"
         |]))
      cursor;
    C.truncate cursor;
    C.close cursor;
    let t2 = Unix.time () in
    (* Unix.sleepf 0.5; *)
    let t3 = Unix.time () in
    Printf.eprintf "[t-finish] serialization: %fs, gc: %fs\n%!" (t2 -. t1) (t3 -. t1)
  | Deserialize ->
    let t1 = Unix.time () in
    let columns, stream = TestDeserializer.deserialize cursor in
    stream
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
         columns;
    let t2 = Unix.time () in
    (* Unix.sleep 1; *)
    (* Gc.full_major (); *)
    let t3 = Unix.time () in
    (* Unix.sleepf 0.5; *)
    Printf.eprintf "[t-finish] deserialization: %fs, gc: %fs\n%!" (t2 -. t1) (t3 -. t1)
;;
