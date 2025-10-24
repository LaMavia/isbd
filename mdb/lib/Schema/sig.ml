type offset = int

module type SchemaSig = sig
  type t

  val serialize : SigArray.t -> offset -> (t * offset, Error.t) result
  val deserialize : SigArray.t -> t -> offset -> offset
end

module CombineSig (A : SchemaSig) (B : SchemaSig) : sig
  include SchemaSig with type t := A.t * B.t
end = struct
  let serialize a offset =
    match A.serialize a offset with
    | Result.Error e -> Result.Error e
    | Result.Ok (av, offset') -> (
        match B.serialize a offset' with
        | Result.Error e -> Result.Error e
        | Result.Ok (bv, offset'') -> Result.Ok ((av, bv), offset''))

  let deserialize a (av, bv) offset =
    A.deserialize a av offset |> B.deserialize a bv
end

module Syntax = struct
  let ( let* ) = Result.bind
  let ( let+ ) = Result.map
end
