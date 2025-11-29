open Bigarray

type col =
  [ `ColVarchar
  | `ColInt
  ]

let col_constr_of_type = function
  | '\001' -> Option.some `ColInt
  | '\002' -> Option.some `ColVarchar
  | _ -> Option.None
;;

let byte_of_col = function
  | `ColInt -> '\001'
  | `ColVarchar -> '\002'
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

  (** [deserialize_dispenser bfs bi ()] changes the position of  [bfs.(bi)], but not the length *)
  val deserialize_dispenser : Stateful_buffers.t -> int -> unit -> t option

  (** Changes the length to the target length, and pos = 0. Reads only the fraglen bytes from the src buffer. Can reassign the buffer *)
  val decode_fragments : Stateful_buffers.t -> int -> int array -> int -> unit

  (** resulting position is the saved fraglen *)
  val encode_fragments : Stateful_buffers.t -> int -> unit

  val deserialize_seq : Stateful_buffers.t -> int -> t Seq.t
end

module Deserializers = struct
  module IntDeserializer : ColumnDeserializer with type t := int64 = struct
    open Stateful_buffers

    let physical_length = 1

    let rec deserialize_dispenser bfs bi =
      deserialize_aux @@ Stateful_buffers.get_buffer bfs bi

    and deserialize_seq bfs bi = Seq.of_dispenser (deserialize_dispenser bfs bi)

    and deserialize_aux a () =
      if a.position >= a.length
      then Option.None
      else (
        let r = get_int64_be a.buffer a.position in
        a.position <- a.position + 8;
        Option.some r)
    ;;

    let rec decode_fragments bfs bi flens fi =
      let enc_len = flens.(fi) in
      let a = get_buffer bfs bi in
      a.length <- enc_len;
      let decoded_values =
        Seq.of_dispenser (decode_vle a) |> Seq.map (decode_delta ()) |> List.of_seq
      in
      let a' = create_bytes (List.length decoded_values * 8) in
      List.iteri (fun i n -> set_int64_be a' (i * 8) n) decoded_values;
      set_buffer bfs bi a';
      (get_buffer bfs bi).length <- Array1.dim a'

    and decode_vle a =
      let pos = ref 0 in
      fun () ->
        if !pos >= a.length then Option.None else decode_vle_head a pos |> Option.some

    and decode_vle_head a pos =
      let octet = Stateful_buffers.get_uint8 a.buffer !pos in
      let continue = octet >= 0b10000000
      and sign = if Int.logand octet 0b01000000 > 0 then -1L else 1L
      and rest = Int.logand octet 0b00111111 in
      let u = rest |> Int64.of_int in
      pos := !pos + 1;
      if continue then decode_vle_tail sign u 6 a pos else Int64.mul sign u

    and decode_vle_tail sign u shift a pos =
      let octet = Stateful_buffers.get_uint8 a.buffer !pos in
      let continue = octet >= 0b10000000
      and rest = Int.logand octet 0b01111111 in
      let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
      pos := !pos + 1;
      if continue then decode_vle_tail sign u (shift + 7) a pos else Int64.mul sign u

    and decode_delta () =
      let last = ref 0L in
      fun v ->
        last := Int64.add v !last;
        !last
    ;;

    let rec encode_fragments bfs bi =
      let open Array1 in
      let a = get_buffer bfs bi in
      let max_enc_len = Const.max_uint_len * (dim a.buffer / 8) in
      let enc_a = create_stb max_enc_len max_enc_len in
      Seq.of_dispenser (encode_delta a) |> Seq.iter (encode_vle enc_a);
      blit (sub enc_a.buffer 0 enc_a.position) (sub a.buffer 0 enc_a.position);
      a.position <- enc_a.position

    and encode_delta a =
      let last = ref 0L
      and pos = ref 0 in
      fun () ->
        if !pos >= a.position
        then Option.None
        else (
          let v = get_int64_be a.buffer !pos in
          let r = Int64.sub v !last in
          last := v;
          pos := !pos + 8;
          Option.Some r)

    and encode_vle a v = encode_vle_head a (v < 0L) (Int64.abs v)

    and encode_vle_head (a : stb) is_neg v =
      let continue_mask = if v > 63L then 0b10000000 else 0b0 (*2^6 - 1*)
      and sign_mask = if is_neg then 0b01000000 else 0b0
      and octet_val = Int64.logand v 0b00111111L |> Int64.to_int in
      let bts = Bytes.make 1 '\000' in
      Bytes.set_uint8 bts 0 Int.(continue_mask |> logor sign_mask |> logor octet_val);
      Stateful_buffers.write_bytes a.buffer a.position 1 bts;
      a.position <- a.position + 1;
      if continue_mask > 0
      then encode_vle_tail a (Int64.shift_right_logical v 6) bts
      else ()

    and encode_vle_tail a v bts =
      let continue_mask = if v > 127L then 0b10000000 else 0b0 (*2^7 - 1*)
      and octet_val = Int64.logand v 0b01111111L |> Int64.to_int in
      Bytes.set_uint8 bts 0 Int.(logor continue_mask octet_val);
      Stateful_buffers.write_bytes a.buffer a.position 1 bts;
      a.position <- a.position + 1;
      if continue_mask > 0
      then encode_vle_tail a (Int64.shift_right_logical v 7) bts
      else ()
    ;;
  end

  module VarcharDeserializer : ColumnDeserializer with type t := string = struct
    open Stateful_buffers

    let physical_length = 2

    let rec deserialize_dispenser bfs bi =
      let uint_dispenser = IntDeserializer.deserialize_dispenser bfs (bi + 1) in
      fun () -> Option.map (deserialize_str @@ get_buffer bfs bi) (uint_dispenser ())

    and deserialize_seq bfs bi = Seq.of_dispenser (deserialize_dispenser bfs bi)

    and deserialize_str a len =
      let ilen = Int64.to_int len in
      let r = read_bytes a.buffer a.position ilen in
      a.position <- a.position + ilen;
      r |> String.of_bytes
    ;;

    let decode_fragments bfs bi flens fi =
      IntDeserializer.decode_fragments bfs (bi + 1) flens (fi + 1);
      let total_str_length =
        IntDeserializer.deserialize_seq bfs (bi + 1)
        |> Seq.fold_left Int64.add 0L
        |> Int64.to_int
      in
      let str_a = get_buffer bfs bi in
      let decompressed_strings =
        LZ4.Bigbytes.decompress
          ~length:total_str_length
          (Array1.sub str_a.buffer 0 flens.(fi))
      in
      let len = Array1.dim decompressed_strings in
      if len > str_a.length
      then set_buffer bfs bi decompressed_strings
      else Array1.(blit decompressed_strings (sub str_a.buffer 0 len))
    ;;

    let encode_fragments bfs bi =
      let open Array1 in
      let str_a = get_buffer bfs bi in
      let compressed_strings =
        LZ4.Bigbytes.compress (sub str_a.buffer 0 str_a.position)
      in
      let len = dim compressed_strings in
      blit compressed_strings (sub str_a.buffer 0 len);
      str_a.position <- len;
      str_a.length <- len;
      IntDeserializer.encode_fragments bfs (bi + 1)
    ;;
  end

  module ColumnInfoDeserializer : ColumnDeserializer with type t := string * col = struct
    let physical_length = 2

    let rec deserialize_dispenser bfs bi =
      let open Utils.Mopt in
      let vchar_dispenser = VarcharDeserializer.deserialize_dispenser bfs bi in
      fun () ->
        let* s = vchar_dispenser () in
        let slen = String.length s - 1 in
        let name = String.sub s 0 slen in
        let* typ = String.get s slen |> col_constr_of_type in
        Option.some (name, typ)

    and deserialize_seq bfs bi = Seq.of_dispenser @@ deserialize_dispenser bfs bi

    let decode_fragments bfs bi flens fi =
      VarcharDeserializer.decode_fragments bfs bi flens fi
    ;;

    (* Stateful_buffers.print_buffers "decoded column info" bfs *)

    let encode_fragments bfs bi = VarcharDeserializer.encode_fragments bfs bi
  end
end

module type ColumnSerializer = sig
  type t

  val physical_length : int
  val serialize : t -> Stateful_buffers.t -> int -> unit
end

module Serializers = struct
  module IntSerializer : ColumnSerializer with type t := int64 = struct
    let physical_length = 1

    let serialize v bfs bi =
      let open Stateful_buffers in
      let a = get_buffer bfs bi in
      set_int64_be a.buffer a.position v;
      a.position <- a.position + 8
    ;;
  end

  module VarcharSerializer : ColumnSerializer with type t := string = struct
    let physical_length = 2

    let rec serialize v bfs bi =
      let vlen = String.length v in
      IntSerializer.serialize (Int64.of_int vlen) bfs (bi + 1);
      serialize_str v vlen (Stateful_buffers.get_buffer bfs bi)

    and serialize_str v vlen a =
      String.iteri (fun i c -> Array1.set a.buffer (a.position + i) c) v;
      a.position <- a.position + vlen
    ;;
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

    (** TODO: Add error handling, and dependency on [V] *)
    let serialize_mut : Data.Types.t -> Stateful_buffers.t -> int -> unit =
      fun d bfs bi ->
      match d with
      | `DataInt i -> Serializers.IntSerializer.serialize i bfs bi
      | `DataVarchar s -> Serializers.VarcharSerializer.serialize s bfs bi
    ;;

    let decode_fragments = V.decode_fragments
    let encode_fragments = V.encode_fragments
    let deserialize_iter bfs bi = V.deserialize_seq bfs bi |> Seq.map V.to_data
  end

module VarcharColDesc : ColDesc = struct
  include Deserializers.VarcharDeserializer

  type t = string

  let to_data s = `DataVarchar s
end

module VarcharLogCol = MakeLogCol (VarcharColDesc)

module IntColDesc : ColDesc = struct
  include Deserializers.IntDeserializer

  type t = int64

  let to_data i = `DataInt i
end

module IntLogCol = MakeLogCol (IntColDesc)
