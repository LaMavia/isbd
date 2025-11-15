let print_hex_bytes b =
  Bytes.iter (fun c -> Printf.eprintf "%02x " (Char.code c)) b;
  Printf.eprintf "\n"
