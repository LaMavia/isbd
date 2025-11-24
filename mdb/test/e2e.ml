open Lib

module Testable = struct
  let data_record =
    let open Data in
    let open Data.Types in
    let pp_data_type ppf = function
      | DataInt i -> Format.pp_print_int ppf @@ Int64.to_int i
      | DataVarchar s -> Format.pp_print_string ppf s
    in
    let pp_data_record ppf (d : data_record) = Format.pp_print_array pp_data_type ppf d in
    Alcotest.testable pp_data_record ( = )
  ;;

  let columns =
    let open Format in
    let pp_column ppf = function
      | name, `ColInt ->
        pp_print_string ppf name;
        print_space ();
        pp_print_string ppf "INT64"
      | name, `ColString ->
        pp_print_string ppf name;
        print_space ();
        pp_print_string ppf "VARCHAR"
    in
    let pp_columns = Format.pp_print_array pp_column in
    Alcotest.testable pp_columns ( = )
  ;;
end

let test_e2e =
  (* let open Data.Types in *)
  let module C = Cursor.StringCursor in
  let module TestSerializer = Serializer.Make (C) in
  let module TestDeserializer = Deserializer.Make (C) in
  let test ~name data cols ~buffer_size =
    ( Printf.sprintf "%s" name
    , `Quick
    , fun () ->
        let cursor = C.create "" |> Result.get_ok in
        TestSerializer.serialize buffer_size cols data cursor;
        Utils.Debugging.print_hex_bytes "cursor after ser" (C.to_bytes cursor);
        cursor |> C.seek 0 |> ignore;
        let got_cols, got_data = TestDeserializer.deserialize cursor in
        Alcotest.(check Testable.columns) "same columns" cols got_cols;
        Alcotest.(check (seq Testable.data_record)) "same records" data got_data )
  in
  [ test ~name:"Empty" (List.to_seq []) [||] ~buffer_size:10 ]
;;

(* [ test ~name:"int" (VInt 2) ~expected:2. *)
(* ; test ~name:"float" (VFloat 3.5) ~expected:3.5 *)
(* ] *)

let suite = [ "end-to-end", test_e2e ]
let () = Alcotest.run __FILE__ suite
