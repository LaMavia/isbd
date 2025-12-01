type t =
  | Int64
  | Varchar

let t_of_yojson = function
  | `String "INT64" -> Int64
  | `String "VARCHAR" -> Varchar
  | v ->
    Yojson.json_error
    @@ Printf.sprintf
         "Expected INT64 or VARCHAR but got %s instead"
         (Yojson.Safe.to_string v)
;;

let yojson_of_t = function
  | Int64 -> `String "INT64"
  | Varchar -> `String "VARCHAR"
;;

let of_lib (t : Lib.Column.col) : t =
  match t with
  | `ColInt -> Int64
  | `ColVarchar -> Varchar
;;

let to_lib (t : t) : Lib.Column.col =
  match t with
  | Int64 -> `ColInt
  | Varchar -> `ColVarchar
;;
