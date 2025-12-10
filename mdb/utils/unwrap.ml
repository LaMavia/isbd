let option ~message = function
  | Some v -> v
  | None -> raise_notrace (Failure message)
;;

let result ~exc = function
  | Ok v -> v
  | Error e -> raise_notrace (exc e)
;;
