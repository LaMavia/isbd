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
  val deserialize_dispenser : Stateful_buffers.t -> int -> unit -> t option
  val decode_fragments : Stateful_buffers.t -> int -> int array -> int -> unit
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
      let a = get_buffer bfs bi
      and dec_a = create_stb enc_len enc_len in
      a.position <- 0;
      a.length <- enc_len;
      Printf.eprintf
        "[%s] Decoding %d(%d) into %d\n"
        __FUNCTION__
        a.length
        (Array1.dim a.buffer)
        enc_len;
      flush_all ();
      Seq.of_dispenser (decode_vle a) |> Seq.iter (decode_delta dec_a);
      a.length <- enc_len;
      blit_big_bytes dec_a a

    and decode_vle a () =
      if a.position >= a.length then Option.None else decode_vle_head a |> Option.some

    and decode_vle_head a =
      let octet = Stateful_buffers.get_uint8 a.buffer a.position in
      let continue = octet >= 0b10000000
      and sign = if Int.logand octet 0b01000000 > 0 then -1L else 1L
      and rest = Int.logand octet 0b00111111 in
      let u = rest |> Int64.of_int in
      a.position <- a.position + 1;
      if continue then decode_vle_tail sign u 6 a else Int64.mul sign u

    and decode_vle_tail sign u shift a =
      let octet = Stateful_buffers.get_uint8 a.buffer a.position in
      let continue = octet >= 0b10000000
      and rest = Int.logand octet 0b01111111 in
      let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
      a.position <- a.position + 1;
      if continue then decode_vle_tail sign u (shift + 7) a else Int64.mul sign u

    and decode_delta a =
      let last = ref 0L in
      fun v ->
        let r = Int64.add v !last in
        last := r;
        Printf.eprintf "[%s] setting [%d, %d]\n" __FUNCTION__ a.position (a.position + 7);
        flush_all ();
        set_int64_be a.buffer a.position r;
        a.position <- a.position + 8
    ;;

    let rec encode_fragments bfs bi =
      let a = get_buffer bfs bi in
      let enc_a = create_stb a.length (Array1.dim a.buffer) in
      a.position <- 0;
      Seq.of_dispenser (encode_delta a) |> Seq.iter (encode_vle enc_a);
      blit_big_bytes enc_a a

    and encode_delta a =
      let last = ref 0L in
      fun () ->
        if a.position >= a.length
        then Option.None
        else (
          let v = get_int64_be a.buffer a.position in
          let r = Int64.sub v !last in
          Printf.eprintf "[%s] encoding %Ld, %Ld -> %Ld\n" __FUNCTION__ v !last r;
          last := v;
          a.position <- a.position + 8;
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
    let physical_length = 2

    let rec deserialize_dispenser bfs bi =
      let uint_dispenser = IntDeserializer.deserialize_dispenser bfs (bi + 1) in
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
      IntDeserializer.decode_fragments bfs (bi + 1) flens (fi + 1);
      let total_str_length =
        IntDeserializer.deserialize_seq bfs (bi + 1)
        |> Seq.fold_left Int64.add 0L
        |> Int64.to_int
      in
      let str_a = Stateful_buffers.get_buffer bfs bi in
      (* Printf.eprintf *)
      (*   "total_str_length=%d, bi=%d, fi=%d, flen=%d, buffer_len=%d, " *)
      (*   total_str_length *)
      (*   bi *)
      (*   fi *)
      (*   flens.(fi) *)
      (*   (Array1.dim str_a.buffer); *)
      (* Utils.Debugging.print_hex_bytes "bytes" str_a.buffer; *)
      let decompressed_strings =
        LZ4.Bigbytes.decompress
          ~length:total_str_length
          (Array1.sub str_a.buffer 0 flens.(fi))
      in
      let len = Array1.dim decompressed_strings in
      (* Printf.eprintf "len=%d\n\n" len; *)
      Array1.(blit decompressed_strings (sub str_a.buffer 0 len));
      str_a.length <- len
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
      (* Utils.Debugging.print_int_array "[colinfo::decode]" flens; *)
      for i = 0 to 1 do
        (* Printf.eprintf "i=%d\n" i; *)
        let a = Stateful_buffers.get_buffer bfs (bi + i) in
        a.length <- flens.(fi + i)
      done
    ;;

    (* Stateful_buffers.print_buffers "decoded column info" bfs *)

    let encode_fragments bfs bi =
      for i = 0 to 1 do
        let a = Stateful_buffers.get_buffer bfs (bi + i) in
        a.length <- a.position;
        a.position <- 0
      done
    ;;
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
      a.position <- a.position + 8;
      Printf.eprintf
        "[%s] serializing %Ld -> %d[%d, %d]\n"
        __FUNCTION__
        v
        bi
        (a.position - 8)
        (a.position - 1)
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
