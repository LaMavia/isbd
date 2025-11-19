open Bigarray

type col =
  [ `ColString
  | `ColInt
  ]

let col_constr_of_type = function
  | '\001' -> Option.some `ColInt
  | '\002' -> Option.some `ColString
  | _ -> Option.None
;;

let byte_of_col = function
  | `ColInt -> '\001'
  | `ColString -> '\002'
;;

module type LogicalColumn = sig
  type t

  val load_mut : Chunk.t -> Stateful_buffers.t -> int array -> int -> unit
  val serialize_mut : Data.Types.t -> Stateful_buffers.t -> int -> unit
  val decode_fragments : Stateful_buffers.t -> int array -> int -> unit
  val encode_fragments : Stateful_buffers.t -> int -> unit
  val deserialize_iter : Stateful_buffers.t -> Data.Types.t Seq.t
end

module type ColumnDeserializer = sig
  type t

  val physical_length : int
  val deserialize_dispenser : Stateful_buffers.t -> int -> unit -> t option
  val decode_fragments : Stateful_buffers.t -> int -> int array -> int -> unit
  val encode_fragments : Stateful_buffers.t -> int -> unit
  val deserialize_seq : Stateful_buffers.t -> int -> t Seq.t
end

module Deserializers = struct
  module IntDeserializer : sig
    include ColumnDeserializer with type t := int64
  end = struct
    let physical_length = 1

    let rec deserialize_dispenser bfs bi = aux @@ Stateful_buffers.get_buffer bfs bi
    and deserialize_seq bfs bi = Seq.of_dispenser (deserialize_dispenser bfs bi)

    and aux a () =
      if a.position >= a.length then Option.None else deserialize_head a |> Option.some

    and deserialize_head a =
      let octet = Bytes.get_uint8 (Stateful_buffers.read_bytes a.buffer a.position 1) 0 in
      let continue = octet >= 0b10000000
      and sign = if Int.logand octet 0b01000000 > 0 then -1L else 1L
      and rest = Int.logand octet 0b00111111 in
      let u = rest |> Int64.of_int in
      a.position <- a.position + 1;
      if continue then deserialize_tail sign u 6 a else Int64.mul sign u

    and deserialize_tail sign u shift a =
      let octet = Bytes.get_uint8 (Stateful_buffers.read_bytes a.buffer a.position 1) 0 in
      let continue = octet >= 0b10000000
      and rest = Int.logand octet 0b01111111 in
      let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
      a.position <- a.position + 1;
      if continue then deserialize_tail sign u (shift + 7) a else Int64.mul sign u
    ;;

    let decode_fragments _ _ _ _ = ()
    let encode_fragments _ _ = ()
  end

  module UIntDeserializer : sig
    include ColumnDeserializer with type t := int64
  end = struct
    let physical_length = 1

    let rec deserialize_dispenser bfs bi = aux @@ Stateful_buffers.get_buffer bfs bi
    and deserialize_seq bfs bi = Seq.of_dispenser (deserialize_dispenser bfs bi)
    and aux a () = if a.position >= a.length then Option.none else deserialize_aux 0L 0 a

    and deserialize_aux u shift a =
      let octet = Bytes.get_uint8 (Stateful_buffers.read_bytes a.buffer a.position 1) 0 in
      let continue = octet >= 0b10000000
      and rest = Int.logand octet 0b01111111 in
      let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
      a.position <- a.position + 1;
      if continue then deserialize_aux u (shift + 7) a else Option.some u
    ;;

    let decode_fragments _ _ _ _ = ()
    let encode_fragments _ _ = ()
  end

  module VarcharDeserializer : sig
    include ColumnDeserializer with type t := string
  end = struct
    let physical_length = 2

    let rec deserialize_dispenser bfs bi =
      let uint_dispenser = UIntDeserializer.deserialize_dispenser bfs (bi + 1) in
      fun () ->
        Option.map
          (deserialize_str @@ Stateful_buffers.get_buffer bfs bi)
          (uint_dispenser ())

    and deserialize_seq bfs bi = Seq.of_dispenser (deserialize_dispenser bfs bi)

    and deserialize_str a len =
      let ilen = Int64.to_int len in
      let r = Stateful_buffers.read_bytes a.buffer a.position ilen in
      a.position <- a.position + ilen;
      r |> String.of_bytes
    ;;

    let decode_fragments bfs bi flens fi =
      UIntDeserializer.decode_fragments bfs (bi + 1) flens (fi + 1);
      let total_str_length =
        UIntDeserializer.deserialize_seq bfs (bi + 1)
        |> Seq.fold_left Int64.add 0L
        |> Int64.to_int
      in
      let str_a = Stateful_buffers.get_buffer bfs bi in
      let decompressed_strings =
        LZ4.Bigbytes.decompress
          ~length:total_str_length
          (Array1.sub str_a.buffer 0 flens.(fi))
      in
      let len = Array1.dim decompressed_strings in
      Array1.(blit decompressed_strings (sub str_a.buffer 0 len));
      str_a.position <- len
    ;;

    let encode_fragments bfs bi =
      let open Array1 in
      let str_a = Stateful_buffers.get_buffer bfs bi in
      let compressed_strings =
        LZ4.Bigbytes.compress (sub str_a.buffer 0 str_a.position)
      in
      let len = dim compressed_strings in
      blit compressed_strings (sub str_a.buffer 0 len);
      str_a.position <- len;
      UIntDeserializer.encode_fragments bfs (bi + 1)
    ;;
  end

  module ColumnInfoDeserializer : sig
    include ColumnDeserializer with type t := string * col
  end = struct
    let physical_length = 2

    let rec deserialize_dispenser bfs bi =
      let open Utils.Mopt in
      let vchar_dispender = VarcharDeserializer.deserialize_dispenser bfs bi in
      fun () ->
        let* s = vchar_dispender () in
        let slen = String.length s - 1 in
        let name = String.sub s 0 slen in
        let* typ = String.get s slen |> col_constr_of_type in
        Option.some (name, typ)

    and deserialize_seq bfs bi = Seq.of_dispenser @@ deserialize_dispenser bfs bi

    let decode_fragments = VarcharDeserializer.decode_fragments
    let encode_fragments = VarcharDeserializer.encode_fragments
  end
end

module type ColumnSerializer = sig
  type t

  val physical_length : int
  val serialize : t -> Stateful_buffers.t -> int -> unit
end

module Serializers = struct
  module IntSerializer : sig
    include ColumnSerializer with type t := int64
  end = struct
    let physical_length = 1

    let rec serialize v bfs bi =
      serialize_head (Stateful_buffers.get_buffer bfs bi) (v < 0L) (Int64.abs v)

    and serialize_head a is_neg v =
      let continue_mask = if v > 63L then 0b10000000 else 0b0 (*2^6 - 1*)
      and sign_mask = if is_neg then 0b01000000 else 0b0
      and octet_val = Int64.logand v 0b00111111L |> Int64.to_int in
      let bts = Bytes.make 1 '\000' in
      Bytes.set_uint8 bts 0 Int.(continue_mask |> logor sign_mask |> logor octet_val);
      Stateful_buffers.write_bytes a.buffer a.position 1 bts;
      a.position <- a.position + 1;
      if continue_mask > 0
      then serialize_tail a (Int64.shift_right_logical v 6) bts
      else ()

    and serialize_tail a v bts =
      let continue_mask = if v > 127L then 0b10000000 else 0b0 (*2^7 - 1*)
      and octet_val = Int64.logand v 0b01111111L |> Int64.to_int in
      Bytes.set_uint8 bts 0 Int.(logor continue_mask octet_val);
      Stateful_buffers.write_bytes a.buffer a.position 1 bts;
      a.position <- a.position + 1;
      if continue_mask > 0
      then serialize_tail a (Int64.shift_right_logical v 7) bts
      else ()
    ;;
  end

  module UIntSerializer : sig
    include ColumnSerializer with type t := int64
  end = struct
    let physical_length = 1

    let rec serialize v bfs bi =
      let bts = Bytes.make 1 '\000' in
      serialize_aux (Stateful_buffers.get_buffer bfs bi) v bts

    and serialize_aux a v bts =
      let continue_mask = if v > 127L then 0b10000000 else 0b0 (*2^7 - 1*)
      and octet_val = Int64.logand v 0b01111111L |> Int64.to_int in
      Bytes.set_uint8 bts 0 Int.(logor continue_mask octet_val);
      Stateful_buffers.write_bytes a.buffer a.position 1 bts;
      a.position <- a.position + 1;
      if continue_mask > 0
      then serialize_aux a (Int64.shift_right_logical v 7) bts
      else ()
    ;;
  end

  module VarcharSerializer : sig
    include ColumnSerializer with type t := string
  end = struct
    let physical_length = 2

    let rec serialize v bfs bi =
      let vlen = String.length v in
      UIntSerializer.serialize (Int64.of_int vlen) bfs (bi + 1);
      serialize_str v vlen (Stateful_buffers.get_buffer bfs bi)

    and serialize_str v vlen a =
      String.iteri (fun i c -> Array1.set a.buffer (a.position + i) c) v;
      a.position <- a.position + vlen
    ;;
    (* Printf.eprintf "[serialize_str] after: %s;" v; *)
    (* Utils.Debugging.print_hex_bytes "BUFFER" a.buffer *)
  end

  module ColumnInfoSerializer : sig
    include ColumnSerializer with type t := string * col
  end = struct
    let physical_length = 2

    let serialize (s, t) bfs bi =
      let s' = String.cat s @@ String.make 1 @@ byte_of_col t in
      VarcharSerializer.serialize s' bfs bi
    ;;
  end
end

module type ColDesc = sig
  include ColumnDeserializer

  val to_data : t -> Data.Types.t
end

module MakeLogCol =
functor
  (V : ColDesc)
  ->
  struct
    let physical_length = V.physical_length

    let load_mut chunk bfs frag_lengths frag_i =
      for i = 0 to physical_length do
        let buffer = Stateful_buffers.get_buffer bfs (frag_i + i) in
        let len = frag_lengths.(frag_i + i) in
        Array1.(blit Chunk.(sub chunk.data chunk.pos len) buffer.buffer);
        Chunk.(chunk.pos <- chunk.pos + len)
      done
    ;;

    let serialize_mut : Data.Types.t -> Stateful_buffers.t -> int -> unit =
      fun d bfs bi ->
      let open Data.Types in
      match d with
      | DataInt i -> Serializers.IntSerializer.serialize i bfs bi
      | DataVarchar s -> Serializers.VarcharSerializer.serialize s bfs bi
    ;;

    let decode_fragments = V.decode_fragments
    let encode_fragments = V.encode_fragments
    let deserialize_iter bfs bi = V.deserialize_seq bfs bi |> Seq.map V.to_data
  end

module VarcharColDesc : ColDesc = struct
  include Deserializers.VarcharDeserializer

  type t = string

  let to_data s = Data.Types.DataVarchar s
end

module VarcharLogCol = MakeLogCol (VarcharColDesc)

module IntColDesc : ColDesc = struct
  include Deserializers.IntDeserializer

  type t = int64

  let to_data i = Data.Types.DataInt i
end

module IntLogCol = MakeLogCol (IntColDesc)
