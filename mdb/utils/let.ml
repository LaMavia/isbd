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
  let ( let* ) o f = bind f o
  let handler err_handler o f = fold ~error:err_handler ~ok:f o
end
