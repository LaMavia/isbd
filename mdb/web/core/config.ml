type t =
  { table_directory : string
  ; result_directory : string
  }

let of_env () =
  let self =
    { table_directory = Sys.getenv "TABLE_DIRECTORY"
    ; result_directory = Sys.getenv "RESULT_DIRECTORY"
    }
  in
  assert (Sys.is_directory self.table_directory);
  assert (Sys.is_directory self.result_directory);
  self
;;
