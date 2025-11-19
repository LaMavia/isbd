type offset = int
type arr = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

module OffsetSyntax = struct
  let ( <+> ) (x1, y1) (x2, y2) = x1 + x2, y1 + y2
  let ( +> ) (x, y) dy = x, y + dy
  let ( <+ ) (x, y) dx = x + dx, y
end

module type Storage = sig
  type t

  val get : t -> int -> int -> bytes
  val set : t -> int -> int -> bytes -> unit
end

module ManagedMMap = struct
  let page_size = 16 * 1024 * 1024
  let page_size_f = float_of_int page_size

  type arr = (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Genarray.t

  type t =
    { fd : Unix.file_descr
    ; mutable size : int
    ; mutable array : arr
    }

  let map_file_descr fd = Unix.map_file fd Bigarray.Char Bigarray.C_layout true [| -1 |]

  let of_file_descr fd =
    let open Unix in
    { fd; array = map_file_descr fd; size = (fstat fd).st_size }
  ;;

  let of_path path =
    let open Unix in
    openfile path [ O_RDWR ] 0o640 |> of_file_descr
  ;;

  let get a offset length =
    let open Bigarray.Genarray in
    Bytes.init length (fun i -> get a.array [| offset + i |])
  ;;

  let set a offset length =
    if offset + length >= a.size
    then (
      let extend_size =
        page_size
        * (int_of_float @@ ceil @@ (float_of_int (offset + length - a.size) /. page_size_f)
          )
      in
      let len_written =
        Unix.lseek a.fd 0 Unix.SEEK_END |> ignore;
        let r = Unix.write a.fd (Bytes.make extend_size '\000') 0 extend_size in
        r
      in
      a.size <- a.size + len_written;
      a.array <- map_file_descr a.fd;
      Gc.major ());
    let open Bigarray.Genarray in
    Bytes.iteri (fun i b -> if i < length then set a.array [| offset + i |] b else ())
  ;;
end

module MMapStorage : sig
  include Storage with type t := ManagedMMap.t
end = struct
  let get = ManagedMMap.get
  let set = ManagedMMap.set
end

(* module Make (Storage : sig *)
(*   type t *)
(**)
(*   (* t -> offset -> bytees -> () *) *)
(*   val dump_buffer : t -> int -> bytes -> int *)
(**)
(*   (* t -> offset -> bytes  *) *)
(*   val load_buffer : t -> int -> bytes *)
(* end) : sig *)
(*   type t *)
(**)
(*   val make : rows:int -> columns:int -> storage:Storage.t -> t *)
(*   val append : t -> row:int -> bytes -> unit *)
(*   val read : t -> row:int -> len:int -> bytes *)
(* end = struct *)
(*   type buffer = { mutable position : int; mutable data : bytes } *)
(**)
(*   type t = { *)
(*     mutable buffers : buffer array; *)
(*     buffer_length : int; *)
(*     storage : Storage.t; *)
(*   } *)
(**)
(*   let make = Utils.Undefined.undefined *)
(*   let append = Utils.Undefined.undefined *)
(*   let read = Utils.Undefined.undefined *)
(* end *)

type t

let read_bytes : arr -> offset -> int -> bytes =
  fun a i0 length ->
  let open Bigarray.Array1 in
  Bytes.init length (fun i -> get a (i0 + i))
;;

let write_bytes : arr -> offset -> int -> bytes -> unit =
  fun a i0 length ->
  let open Bigarray.Array1 in
  Bytes.iteri (fun i b -> if i < length then set a (i0 + i) b else ())
;;

(*
   Deserializer pisze do buffera.
  Jeśli buffer się przepełni (boundary condition check), buffer zapisuje do storage'a.

  Serializer czyta z buffera.
  Jeśli jest próba odczytania offsetu, którego nie mamy w bufferze, zdumpuj obecny buffer i wczytaj odpowiedni buffer.
*)
