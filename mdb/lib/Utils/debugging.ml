open Bigarray

let print_hex_bytes prefix a =
  let b = Bytes.init (Array1.dim a) (Array1.get a) in
  Printf.eprintf "%s (%d):\n" prefix (Bytes.length b);
  Bytes.iter (fun c -> Printf.eprintf "%03d " (Char.code c)) b;
  Printf.eprintf "\n";
  flush_all ()
;;

let print_int_array label arr =
  Printf.eprintf "%s: " label;
  Array.iter (Printf.eprintf "%d ") arr;
  Printf.eprintf "\n";
  flush_all ()
;;

(*
   .
 8 | 000 000 000 000 000 000 000 100 : suggested buffer size = 100 
16 | 000 000 000 000 000 000 000 010 
24 |  010 000 : fraglens (10 - 0), (10 - 10)
32 | 000 002 002 002 002 002 002 002 002 002 
40 | 001 002 002 002 002 002 002 002 002 002 
48 | columns:
56 |  105 110 116 049 001 
64 |  105 110 116 050 001 
72 | 000 000 000 000 000 000 000 005 
80 | 000 000 000 000 000 000 000 005 
88 | 000 000 000 000 000 000 000 038 
96 | 000 000 000 000 000 000 000 048


 8 | 000 000 000 000 000 000 000 100 
16 | 000 000 000 000 000 000 000 012 
24 | 011 073 007 071 160 076 111 114 
32 | 101 109 068 111 108 111 114 005 
40 | 000 128 073 112 115 117 109 083 
48 | 105 116 005 066 118 099 049 002 
56 | 118 099 050 002 
   | 000 000 000 000 000 000 000 004 
   | 000 000 000 000 000 000 000 004 
   | 000 000 000 000 000 000 000 044 
   | 000 000 000 000 000 000 000 052
*)
