module SignedVLQSig : sig
  include Sig.SchemaSig with type t := int64
end = struct
  (*
      | 7 6 5 4 3 2 1 0 | 7 6 5 4 3 2 1 0 | exponent of 2
      | A B ___________ | A _____________ |
      |   first byte    |   other bytes   |

    A = [continue]
    B = sign
  *)

  let rec serialize a offset = serialize_head a offset

  and serialize_head a offset =
    let octet = Bytes.get_uint8 (SigArray.read_bytes a offset 1) 0 in
    let continue = octet >= 0b10000000
    and sign = if Int.logand octet 0b01000000 > 0 then -1L else 1L
    and rest = Int.logand octet 0b00111111 in
    let u = rest |> Int64.of_int in
    Printf.eprintf "octet=%du | continue=%B | sign=%Ld\n" octet continue sign;
    if continue then serialize_tail sign u 6 a (offset + 1)
    else Result.ok (Int64.mul sign u, offset + 1)

  and serialize_tail sign u shift a offset =
    let octet = Bytes.get_uint8 (SigArray.read_bytes a offset 1) 0 in
    let continue = octet >= 0b10000000 and rest = Int.logand octet 0b01111111 in
    let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
    Printf.eprintf "octet=%du | shift=%d | continue=%B\n" octet shift continue;
    if continue then serialize_tail sign u (shift + 7) a (offset + 1)
    else Result.ok (Int64.mul sign u, offset + 1)

  let rec deserialize a v offset =
    deserialize_head a (v < 0L) (Int64.abs v) offset

  and deserialize_head a is_neg v offset =
    let continue_mask = if v > 63L then 0b10000000 else 0b0 (*2^6 - 1*)
    and sign_mask = if is_neg then 0b01000000 else 0b0
    and octet_val = Int64.logand v 0b00111111L |> Int64.to_int in
    let bts = Bytes.make 1 '\000' in
    Bytes.set_uint8 bts 0
      Int.(continue_mask |> logor sign_mask |> logor octet_val);
    SigArray.write_bytes a offset 1 bts;
    if continue_mask > 0 then
      deserialize_tail a (Int64.shift_right_logical v 6) (offset + 1)
    else offset + 1

  and deserialize_tail a v offset =
    let continue_mask = if v > 127L then 0b10000000 else 0b0 (*2^7 - 1*)
    and octet_val = Int64.logand v 0b01111111L |> Int64.to_int in
    let bts = Bytes.make 1 '\000' in
    Bytes.set_uint8 bts 0 Int.(logor continue_mask octet_val);
    SigArray.write_bytes a offset 1 bts;
    if continue_mask > 0 then
      deserialize_tail a (Int64.shift_right_logical v 7) (offset + 1)
    else offset + 1
end

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

module ListSigMake (S : Sig.SchemaSig) : sig
  include Sig.SchemaSig with type t := S.t list
end = struct
  let serialize a offset =
    let open Sig.Syntax in
    let* list_len, offset = IntSig.serialize a offset in
    let rec aux (u : S.t list) (left : int) offset =
      match left with
      | 0 -> Result.Ok (List.rev u, offset)
      | left ->
          let* elem, offset = S.serialize a offset in
          aux (elem :: u) (left - 1) offset
    in
    match list_len with
    | n when n < 0 ->
        Result.Error
          { offset; reason = Printf.sprintf "Negative list length: %d" n }
    | 0 -> Result.Ok ([], offset)
    | n -> aux [] n offset

  let deserialize a (lst : S.t list) offset =
    let offset = IntSig.deserialize a (List.length lst) offset in
    List.fold_left (Fun.flip @@ S.deserialize a) offset lst
end

module EphSeqSigMake (S : Sig.SchemaSig) : sig
  include Sig.SchemaSig with type t := S.t Seq.t
end = struct
  let serialize a offset =
    let open Sig.Syntax in
    let* final_offset, offset = IntSig.serialize a offset in
    let mk_dispenser () =
      let offset = ref offset in
      fun () ->
        if !offset >= final_offset then Option.None
        else
          match S.serialize a !offset with
          | Result.Error _ -> Option.None
          | Result.Ok (elem, offset') ->
              offset := offset';
              Option.Some elem
    in
    Result.Ok (Seq.of_dispenser @@ mk_dispenser (), final_offset)

  let deserialize a (seq : S.t Seq.t) offset =
    let final_offset =
      Seq.fold_left
        (fun elem offset -> S.deserialize a offset elem)
        (offset + IntSig.size) seq
    in
    IntSig.deserialize a final_offset offset |> ignore;
    final_offset
end

module PersonListSig = ListSigMake (struct
  type t = person

  include PersonSig
end)

module PersonEphSeqSig = EphSeqSigMake (struct
  type t = person

  include PersonSig
end)
