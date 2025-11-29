let alt (fs : ('a -> 'b) list) (v : 'a) : 'b =
  let rec aux fs e =
    match fs with
    | [] -> raise e
    | f :: fs' ->
      (try f v with
       | e' -> aux fs' e')
  in
  aux fs (Invalid_argument "Empty alternative")
;;
