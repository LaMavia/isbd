let are_unique xs =
  let seen = Hashtbl.create (List.length xs) in
  let rec aux = function
    | [] -> Ok ()
    | x :: xs ->
      if Hashtbl.mem seen x
      then Error x
      else (
        Hashtbl.add seen x ();
        aux xs)
  in
  aux xs
;;
