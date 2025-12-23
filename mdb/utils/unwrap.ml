let option ~message = function
  | Some v -> v
  | None -> raise (Failure message)
;;

let result ~exc = function
  | Ok v -> v
  | Error e -> raise (exc e)
;;
