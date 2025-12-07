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
end
