open Lib
open Bigarray

let () =
  let module TestSerializer = Serializer.Make (Cursor.StringCursor) in
  let module TestDeserializer = Deserializer.Make (Cursor.StringCursor) in
  let output_cursor = Cursor.StringCursor.create "" |> Result.get_ok in
  let cols =
    [| "col_int_1", `ColInt
     ; "col_int_2", `ColInt
     ; "col_vchar_1", `ColString
     ; "col_vchar_2", `ColString
    |]
  in
  TestSerializer.serialize
    100
    cols
    (List.init 2 (fun i ->
       Data.Types.
         [| DataInt (Int64.mul 22222L (Int64.of_int i))
          ; DataInt (Int64.mul 5345431L (Int64.of_int i))
          ; DataVarchar (Printf.sprintf "Hello %d" i)
          ; DataVarchar (List.init i (Fun.const ":3") |> String.concat "|")
         |])
     |> List.to_seq)
    output_cursor;
  TestSerializer.write_columns cols output_cursor;
  let bts = output_cursor |> Cursor.StringCursor.to_bytes in
  let s = String.init (Array1.dim bts) (Array1.get bts) in
  Printf.eprintf "OUTPUT_TEXT:\n%s\n" s;
  Utils.Debugging.print_hex_bytes "OUTPUT" bts;
  flush_all ();
  Printf.eprintf "\n=========================================\n\n";
  output_cursor |> Cursor.StringCursor.seek 0 |> ignore;
  let columns, chunks_length = TestDeserializer.read_columns output_cursor in
  columns
  |> Array.iter (fun (s, t) ->
    Printf.eprintf
      "%s\t%s\n"
      s
      (match t with
       | `ColInt -> "INT"
       | `ColString -> "VCHAR"));
  TestDeserializer.deserialize output_cursor columns chunks_length
  |> Seq.iteri (fun i r ->
    Printf.eprintf "%02d. " i;
    Array.iter (fun v -> Printf.eprintf "%s " (Data.Types.to_str v)) r;
    Printf.eprintf "\n")
;;
