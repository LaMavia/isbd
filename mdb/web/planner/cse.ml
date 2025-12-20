(* open Models *)
(* open Lib *)

(** [with_cse f e] calls [f self e] where [self] is the memoised [f] function *)
let with_cse f =
  let memo = Hashtbl.create ~random:true 10 in
  let rec aux e =
    match Hashtbl.find_opt memo e with
    | Some v -> v
    | None ->
      let v = f aux e in
      Hashtbl.add memo e v;
      v
  in
  aux
;;
