open Lib
open Bigarray

let () =
  let module TestSerializer = Serializer.Make (Cursor.StringCursor) in
  let module TestDeserializer = Deserializer.Make (Cursor.StringCursor) in
  let output_cursor = Cursor.StringCursor.create "" |> Result.get_ok in
  let cols =
    [| ("some random number", `ColInt); ("fun little string", `ColString) |]
  in
  TestSerializer.serialize 20 cols
    (List.init 10 (fun i ->
         Data.Types.
           [|
             DataInt (Int64.mul 10000L (Int64.of_int i));
             DataVarchar (Printf.sprintf "Hello %d" i);
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
  TestDeserializer.read_columns output_cursor
  |> Array.iter (fun (s, t) ->
         Printf.eprintf "%s\t%s\n" s
           (match t with `ColInt -> "INT" | `ColString -> "VCHAR"))
