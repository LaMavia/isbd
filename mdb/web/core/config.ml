type t =
  { table_directory : string
  ; result_directory : string
  ; metastore_path : string
  }

let of_env () =
  let open Sys in
  let self =
    { table_directory = getenv "TABLE_DIRECTORY"
    ; result_directory = getenv "RESULT_DIRECTORY"
    ; metastore_path = getenv "METASTORE_PATH"
    }
  in
  assert (is_directory self.table_directory);
  assert (is_directory self.result_directory);
  assert (is_regular_file self.metastore_path);
  self
;;
