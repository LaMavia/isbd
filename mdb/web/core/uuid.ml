type t = Uuidm.t

let uuid_length = Uuidm.(nil |> to_string) |> String.length

let t_of_yojson : Yojson.Safe.t -> t = function
  | `String s -> Uuidm.of_string s |> Option.get
  | _ -> Yojson.json_error "Failed to parse uuid"
;;

let yojson_of_t uuid = `String (Uuidm.to_string uuid)
let v4 = Uuidm.v4_gen (Random.State.make_self_init ())
let to_string u = Uuidm.to_string u

let of_string u =
  let invalid_message = Printf.sprintf "invalid uuid %s" u in
  if String.length u <> uuid_length then raise (Invalid_argument invalid_message);
  Uuidm.of_string u |> Utils.Unwrap.option ~message:invalid_message
;;
