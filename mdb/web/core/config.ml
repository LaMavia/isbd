type t =
  { table_directory : string
  ; result_directory : string
  ; data_directory : string
  ; metastore_path : string
  }

let of_env () =
  let open Sys in
  let open Utils in
  let getvar key =
    getenv_opt key
    |> Unwrap.option ~message:(Printf.sprintf "Env variable %s is undefined" key)
  in
  let self =
    { table_directory = getvar "TABLE_DIRECTORY"
    ; result_directory = getvar "RESULT_DIRECTORY"
    ; data_directory = getvar "DATA_DIRECTORY"
    ; metastore_path = getvar "METASTORE_PATH"
    }
  in
  assert (is_directory self.table_directory);
  assert (is_directory self.result_directory);
  assert (is_directory self.data_directory);
  (* allow metastore to not exist yet *)
  self
;;
