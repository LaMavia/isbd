open Bigarray

let print_hex_bytes prefix a =
  let b = Bytes.init (Array1.dim a) (Array1.get a) in
  Printf.eprintf "%s (%d):\n" prefix (Bytes.length b);
  Bytes.iter (fun c -> Printf.eprintf "%02x " (Char.code c)) b;
  Printf.eprintf "\n"
;;

let print_int_array label arr =
  Printf.eprintf "%s: " label;
  Array.iter (Printf.eprintf "%d ") arr;
  Printf.eprintf "\n"
;;
