module type SchemaSig = sig
  type t
  type extra

  val serialize
    :  SigArray.arr
    -> SigArray.offset
    -> extra
    -> (t * SigArray.offset, Error.t) result

  val deserialize : SigArray.arr -> extra -> t -> SigArray.offset -> SigArray.offset
  val compress : bytes -> int -> bytes
  val decompress : bytes -> int -> bytes
end

module Syntax = struct
  let ( let* ) = Result.bind
  let ( let+ ) = Result.map
end
