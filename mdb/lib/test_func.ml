let test_func (logcols : (string * Column.col) array) (s : Data.Types.t array Seq.t) =
  let init_val =
    Array.map
      (function
        | _, `ColInt -> Either.left (0., 0.)
        | _, `ColVarchar -> Either.right (Hashtbl.create 26 : (char, int) Hashtbl.t))
      logcols
  and aux u r =
    Array.iteri
      (fun i c ->
         match c with
         | `DataInt n ->
           let n = Int64.to_float n in
           u.(i)
           <- Either.map_left
                (fun (avg, m) -> (avg *. m /. (m +. 1.)) +. (n /. (m +. 1.)), m +. 1.)
                u.(i)
         | `DataVarchar s ->
           u.(i)
           <- Either.map_right
                (fun dict ->
                   String.iter
                     (fun c ->
                        Hashtbl.replace
                          dict
                          c
                          ((Hashtbl.find_opt dict c |> Option.value ~default:0) + 1))
                     s;
                   dict)
                u.(i)
         | `DataBool b -> raise (Invalid_argument (Printf.sprintf "%b" b)))
      r;
    u
  in
  Seq.fold_left aux init_val s |> Array.map (Either.map_left fst)
;;
