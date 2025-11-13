(*
interface LogicColumn {
  /** Load physical columns from chunk to buffers.
   * `fragment_lengths[fragment_lengths_i + i]` is the length of the i-th physical column's fragment in `chunk`.
   */
  load_mut(
    chunk: Chunk,
    buffers: StatefulBuffers,
    fragment_lengths: size_t[],
    fragment_lengths_i: size_t,
  ): void;

  /** Serialize the data in `cell` into buffers
   * `buffers[buffer_i + i]` for each physical column index `i`.
   */
  serialize_mut(cell: Data, buffers: StatefulBuffers, buffer_i: size_t): void;

  /** Decode (ie. decrypt) data in buffers `buffer[i]`
   * of lengths `fragment_lengths[fragment_lengths_i + i]`
   * for each physical column index `i`.
   */
  decode_fragments(
    buffers: StatefulBuffers,
    fragment_lengths: size_t[],
    fragment_lengths_i: size_t,
  );

  /** Deserialize data in `buffers` into the logical column.
   */
  deserialize_iter(buffers: StatefulBuffers): Iterator<Data>;

  /** Number of physical columns needed to represent the logical column.
   */
  physical_length: size_t;
}
*)

module type LogicalColumn = sig
  type t

  val physical_length : int
  val create : name:string -> t
  val get_name : t -> string
  val load_mut : Chunk.t -> Stateful_buffers.t -> int array -> int -> unit
  val serialize_mut : Data.Types.t -> Stateful_buffers.t -> int -> unit
  val decode_fragments : Stateful_buffers.t -> int array -> int -> unit
  val encode_fragments : Stateful_buffers.t -> int -> unit
  val deserialize_iter : Stateful_buffers.t -> Data.Types.t Seq.t
end

module type ColumnDeserializer = sig
  type t

  val physical_length : int
  val deserialize : Stateful_buffers.t -> int -> t Seq.t
  val decode_fragments : Stateful_buffers.t -> int array -> int -> unit
  val encode_fragments : Stateful_buffers.t -> int -> unit
end

module Deserializers = struct
  module IntDeserializer : sig
    include ColumnDeserializer with type t := int64
  end = struct
    let physical_length = 1

    let rec deserialize bfs bi =
      Seq.of_dispenser (aux @@ Stateful_buffers.get_buffer bfs bi)

    and aux a () =
      if a.position >= Bytes.length a.buffer then Option.None
      else deserialize_head a |> Option.some

    and deserialize_head a =
      let octet = Bytes.get_uint8 a.buffer a.position in
      let continue = octet >= 0b10000000
      and sign = if Int.logand octet 0b01000000 > 0 then -1L else 1L
      and rest = Int.logand octet 0b00111111 in
      let u = rest |> Int64.of_int in
      Printf.eprintf "octet=%du | continue=%B | sign=%Ld\n" octet continue sign;
      a.position <- a.position + 1;
      if continue then deserialize_tail sign u 6 a else Int64.mul sign u

    and deserialize_tail sign u shift a =
      let octet = Bytes.get_uint8 a.buffer a.position in
      let continue = octet >= 0b10000000
      and rest = Int.logand octet 0b01111111 in
      let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
      Printf.eprintf "octet=%du | shift=%d | continue=%B\n" octet shift continue;
      a.position <- a.position + 1;
      if continue then deserialize_tail sign u (shift + 7) a
      else Int64.mul sign u

    let decode_fragments _ _ _ = ()
    let encode_fragments _ _ = ()
  end

  module UIntDeserializer : sig
    include ColumnDeserializer with type t := int64
  end = struct
    let physical_length = 1

    let rec deserialize bfs bi =
      Seq.of_dispenser (aux @@ Stateful_buffers.get_buffer bfs bi)

    and aux a () =
      if a.position >= Bytes.length a.buffer then Option.none
      else deserialize_aux 0L 0 a

    and deserialize_aux u shift a =
      let octet = Bytes.get_uint8 a.buffer a.position in
      let continue = octet >= 0b10000000
      and rest = Int.logand octet 0b01111111 in
      let u = Int64.shift_left (Int64.of_int rest) shift |> Int64.add u in
      Printf.eprintf "octet=%du | shift=%d | continue=%B\n" octet shift continue;
      a.position <- a.position + 1;
      if continue then deserialize_aux u (shift + 7) a else Option.some u

    let decode_fragments _ _ _ = ()
    let encode_fragments _ _ = ()
  end

  module VarcharDeserializer : sig
    include ColumnDeserializer with type t := string
  end = struct
    let physical_length = 2

    let rec deserialize bfs bi =
      UIntDeserializer.deserialize bfs (bi + 1)
      |> Seq.map (deserialize_str @@ Stateful_buffers.get_buffer bfs bi)

    and deserialize_str a len =
      let r = Bytes.sub_string a.buffer a.position (Int64.to_int len) in
      a.position <- a.position + 1;
      r

    let decode_fragments = Utils.Undefined.undefined
    let encode_fragments = Utils.Undefined.undefined
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
      Bytes.set_uint8 a.buffer a.position
        Int.(continue_mask |> logor sign_mask |> logor octet_val);
      a.position <- a.position + 1;
      if continue_mask > 0 then serialize_tail a (Int64.shift_right_logical v 6)
      else ()

    and serialize_tail a v =
      let continue_mask = if v > 127L then 0b10000000 else 0b0 (*2^7 - 1*)
      and octet_val = Int64.logand v 0b01111111L |> Int64.to_int in
      Bytes.set_uint8 a.buffer a.position Int.(logor continue_mask octet_val);
      a.position <- a.position + 1;
      if continue_mask > 0 then serialize_tail a (Int64.shift_right_logical v 7)
      else ()
  end

  module UIntSerializer : sig
    include ColumnSerializer with type t := int64
  end = struct
    let physical_length = 1

    let rec serialize v bfs bi =
      serialize_aux (Stateful_buffers.get_buffer bfs bi) v

    and serialize_aux a v =
      let continue_mask = if v > 127L then 0b10000000 else 0b0 (*2^7 - 1*)
      and octet_val = Int64.logand v 0b01111111L |> Int64.to_int in
      Bytes.set_uint8 a.buffer a.position Int.(logor continue_mask octet_val);
      a.position <- a.position + 1;
      if continue_mask > 0 then serialize_aux a (Int64.shift_right_logical v 7)
      else ()
  end

  module VarcharSerializer : sig
    include ColumnSerializer with type t := string
  end = struct
    let physical_length = 2

    let rec serialize v bfs bi =
      let vlen = String.length v in
      UIntSerializer.serialize (Int64.of_int vlen) bfs (bi + 1);
      serialize_str v vlen (Stateful_buffers.get_buffer bfs bi)

    and serialize_str v vlen a = Bytes.blit_string v 0 a.buffer a.position vlen
  end
end

module Make =
functor
  (V : sig
     include ColumnDeserializer

     val to_data : t -> Data.Types.t
   end)
  ->
  struct
    type t = { name : string }

    let physical_length = V.physical_length
    let create ~name = { name }
    let get_name c = c.name

    let load_mut chunk bfs frag_lengths frag_i =
      for i = 0 to physical_length do
        let buffer = Stateful_buffers.get_buffer bfs (frag_i + i) in
        Bytes.blit
          Chunk.(chunk.data)
          Chunk.(chunk.pos)
          buffer.buffer buffer.position
          frag_lengths.(frag_i + i)
      done

    let serialize_mut d bfs bi =
      let open Data.Types in
      match d with
      | DataInt i -> Serializers.IntSerializer.serialize i bfs bi
      | DataVarchar s -> Serializers.VarcharSerializer.serialize s bfs bi

    let decode_fragments = V.decode_fragments
    let encode_fragments = V.encode_fragments
    let deserialize_iter bfs bi = V.deserialize bfs bi |> Seq.map V.to_data
  end
