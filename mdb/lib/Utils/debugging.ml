open Bigarray

let print_hex_bytes prefix a =
  let b = Stateful_buffers.read_bytes a 0 (Array1.dim a) in
  Printf.eprintf "%s (%d):\n" prefix (Bytes.length b);
  Bytes.iter (fun c -> Printf.eprintf "%02x " (Char.code c)) b;
  Printf.eprintf "\n"
