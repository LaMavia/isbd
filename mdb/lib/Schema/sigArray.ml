type t =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Genarray.t

let read_bytes (a : t) (offset : int) (length : int) =
  let open Bigarray.Genarray in
  Bytes.init length (fun i -> get a [| offset + i |])

let write_bytes (a : t) (offset : int) (length : int) =
  let open Bigarray.Genarray in
  Bytes.iteri (fun i b -> if i < length then set a [| offset + i |] b else ())
