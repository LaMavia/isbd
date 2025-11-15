let print_hex_bytes prefix b =
  Printf.eprintf "%s (%d):\n" prefix (Bytes.length b);
  Bytes.iter (fun c -> Printf.eprintf "%02x " (Char.code c)) b;
  Printf.eprintf "\n"
