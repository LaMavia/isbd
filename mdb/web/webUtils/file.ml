let rec rm_rec path =
  if Sys.is_directory path
  then (
    Sys.readdir path
    |> Array.iter (fun child_path -> rm_rec @@ Filename.concat path child_path);
    Sys.rmdir path)
  else if Sys.is_regular_file path
  then Sys.remove path
;;
