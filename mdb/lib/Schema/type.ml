module IntSig : sig
  val size : int

  include Sig.SchemaSig with type t := int
end = struct
  let size = 8

  let serialize a offset =
    let bytes = SigArray.read_bytes a offset size in
    Result.Ok (Bytes.get_int64_be bytes 0 |> Int64.to_int, offset + size)

  let deserialize a v i =
    let bytes = Bytes.create size in
    Int64.of_int v |> Bytes.set_int64_be bytes 0;
    SigArray.write_bytes a i size bytes;
    i + size
end

module CharSig : sig
  include Sig.SchemaSig with type t := string
end = struct
  let serialize a offset =
    IntSig.serialize a offset
    |> Result.map (fun (len, offset') ->
           let str = SigArray.read_bytes a offset' len |> Bytes.to_string in
           (str, offset' + len))

  let deserialize a str offset =
    let len = String.length str in
    let size = IntSig.size + len in
    let bytes = Bytes.create size in
    Int64.of_int len |> Bytes.set_int64_be bytes 0;
    Bytes.blit_string str 0 bytes IntSig.size len;
    SigArray.write_bytes a offset size bytes;
    offset + size
end

type person = { age : int; name : string; surname : string }

module PersonSig : sig
  include Sig.SchemaSig with type t := person
end = struct
  let serialize a offset =
    let open Sig.Syntax in
    let* age, offset = IntSig.serialize a offset in
    let* name, offset = CharSig.serialize a offset in
    let* surname, offset = CharSig.serialize a offset in
    Result.Ok ({ age; name; surname }, offset)

  let deserialize a p offset =
    IntSig.deserialize a p.age offset
    |> CharSig.deserialize a p.name
    |> CharSig.deserialize a p.surname
end
