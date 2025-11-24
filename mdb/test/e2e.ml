open Lib

module Testable = struct
  let data_record =
    let open Data in
    let pp_data_type ppf = function
      | `DataInt i -> Format.pp_print_int ppf @@ Int64.to_int i
      | `DataVarchar s -> Format.pp_print_string ppf s
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
      | name, `ColVarchar ->
        pp_print_string ppf name;
        print_space ();
        pp_print_string ppf "VARCHAR"
    in
    let pp_columns = Format.pp_print_array pp_column in
    Alcotest.testable pp_columns ( = )
  ;;
end

let test_e2e =
  let module C = Cursor.StringCursor in
  let module TestSerializer = Serializer.Make (C) in
  let module TestDeserializer = Deserializer.Make (C) in
  let test ~name data cols ~buffer_size ?(speed = `Quick) () =
    ( Printf.sprintf "%s" name
    , speed
    , fun () ->
        let cursor = C.create "" |> Result.get_ok in
        TestSerializer.serialize buffer_size cols data cursor;
        cursor |> C.seek 0 |> ignore;
        Printf.eprintf
          "c_r ≈ %f\n"
          (float_of_int (C.len cursor) /. float_of_int (max 1 (Data.approx_size data)));
        let got_cols, got_data = TestDeserializer.deserialize cursor in
        Alcotest.(check Testable.columns) "same columns" cols got_cols;
        Alcotest.(check (seq Testable.data_record)) "same records" data got_data )
  in
  [ test ~name:"Empty" Seq.empty [||] ~buffer_size:10 ()
  ; test
      ~name:"Small ints"
      (List.to_seq [ [| `DataInt 1L; `DataInt 2L |]; [| `DataInt 3L; `DataInt 4L |] ])
      [| "int1", `ColInt; "int2", `ColInt |]
      ~buffer_size:10
      ()
  ; test
      ~name:"Small varchar"
      (List.to_seq
         [ [| `DataVarchar "Lorem"; `DataVarchar "Ipsum" |]
         ; [| `DataVarchar "Dolor"; `DataVarchar "Sit" |]
         ])
      [| "vc1", `ColVarchar; "vc2", `ColVarchar |]
      ~buffer_size:100
      ()
  ; test
      ~name:"Small mixed"
      (List.to_seq
         [ [| `DataVarchar "Zuzanna"; `DataInt 23L |]
         ; [| `DataVarchar "Karolina"; `DataInt 25L |]
         ])
      [| "name", `ColVarchar; "age", `ColInt |]
      ~buffer_size:20
      ()
  ; (let n_cols = 2000
     and n_rows = 500 in
     test
       ~name:(Printf.sprintf "Wide Ints")
       (Seq.init n_rows (fun i ->
          Array.init n_cols (fun j -> `DataInt (Int64.of_int @@ (i * (j + 1))))))
       (Array.init n_cols (fun j -> Printf.sprintf "Column%d" j, `ColInt))
       ~buffer_size:(n_cols * Const.max_uint_len * 2)
       ~speed:`Slow
       ())
  ]
;;

(* [ test ~name:"int" (VInt 2) ~expected:2. *)
(* ; test ~name:"float" (VFloat 3.5) ~expected:3.5 *)
(* ] *)

let suite = [ "end-to-end", test_e2e ]
let () = Alcotest.run __FILE__ suite
