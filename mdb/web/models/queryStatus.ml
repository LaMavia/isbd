type t =
  | Created
  | Planning
  | Running
  | Completed
  | Failed

let t_of_yojson (json : Yojson.Safe.t) : t =
  match json with
  | `String "CREATED" -> Created
  | `String "PLANNING" -> Planning
  | `String "RUNNING" -> Running
  | `String "COMPLETED" -> Completed
  | `String "FAILED" -> Failed
  | v ->
    Yojson.json_error
    @@ Printf.sprintf
         "Expected «CREATED | PLANNING | RUNNING | COMPLETED | FAILED» but got «%s»"
         (Yojson.Safe.to_string v)
;;

let yojson_of_t = function
  | Created -> `String "CREATED"
  | Planning -> `String "PLANNING"
  | Running -> `String "RUNNING"
  | Completed -> `String "COMPLETED"
  | Failed -> `String "FAILED"
;;
