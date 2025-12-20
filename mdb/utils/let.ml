module Opt = struct
  open Option

  let ( let- ) o f = iter f o
  let ( let+ ) o f = map f o
  let ( let* ) o f = bind o f
end

module Res = struct
  open Result

  let ( let- ) o f = iter f o
  let ( let+ ) o f = map f o
  let ( let* ) o f = bind o f
  let handler err_handler o f = fold ~error:err_handler ~ok:f o

  let ( and* ) a b =
    match a, b with
    | Ok ar, Ok br -> Ok (ar, br)
    | Error aerr, Error berr -> Error (List.append aerr berr)
    | Error aerr, Ok _ -> Error aerr
    | Ok _, Error berr -> Error berr
  ;;
end
