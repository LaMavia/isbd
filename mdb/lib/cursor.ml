open Stateful_buffers
open Bigarray

module type CursorInterface = sig
  type t
  type src

  val create : src -> (t, string) result
  val read : int -> t -> big_bytes
  val write : int -> big_bytes -> t -> t
  val move : int -> t -> t
  val seek : int -> t -> t
  val len : t -> int
  val position : t -> int
  val to_bytes : t -> big_bytes
end

module StringCursor = struct
  type t =
    { mutable i : int
    ; mutable buffer : big_bytes
    ; mutable length : int
    }

  type src = string

  let create s =
    Result.ok
      { i = 0; buffer = Stateful_buffers.big_bytes_of_string s; length = String.length s }
  ;;

  let len c = c.length

  let move di c =
    c.i <- c.i + di;
    c
  ;;

  let seek i c =
    c.i <- i;
    c
  ;;

  let read len c =
    let r = Array1.sub c.buffer c.i len in
    move len c |> ignore;
    r
  ;;

  let write len bs c =
    let blen = c.length in
    if c.i + len >= blen
    then (
      let added_len = c.i + len - blen in
      let a' = create_bytes (blen + (added_len * 2)) in
      write_big_bytes a' 0 c.length c.buffer;
      c.buffer <- a');
    write_big_bytes c.buffer c.i len bs;
    c.length <- max (c.i + len) c.length;
    move len c
  ;;

  let position c = c.i
  let to_bytes c = Array1.sub c.buffer 0 c.length
end

module MMapCursor = struct
  module ManagedMMap = struct
    open Bigarray

    let page_size = 64_000_000
    let page_size_f = float_of_int page_size

    type arr = (char, int8_unsigned_elt, c_layout) Array1.t

    type t =
      { fd : Unix.file_descr
      ; mutable size : int
      ; mutable array : arr
      }

    let map_file_descr fd =
      Unix.map_file fd Char C_layout true [| -1 |] |> array1_of_genarray
    ;;

    let of_file_descr fd =
      let open Unix in
      { fd; array = map_file_descr fd; size = (fstat fd).st_size }
    ;;

    let of_path path =
      let open Unix in
      openfile path [ O_RDWR; O_CREAT ] 0o640 |> of_file_descr
    ;;

    let get a offset length = Array1.sub a.array offset length

    let set a offset length bts =
      if offset + length >= a.size
      then (
        let new_size = a.size + max length page_size in
        Unix.ftruncate a.fd new_size;
        a.size <- new_size;
        a.array <- map_file_descr a.fd);
      Array1.(blit bts (sub a.array offset length))
    ;;
  end

  type t =
    { mutable map : ManagedMMap.t
    ; mutable position : int
    ; mutable length : int
    }

  type src = string

  let create path =
    try
      let map = ManagedMMap.of_path path in
      { map; position = 0; length = map.size } |> Result.ok
    with
    | Failure reason -> Result.error reason
  ;;

  let move di c =
    c.position <- c.position + di;
    c.length <- max c.length c.position;
    c
  ;;

  let read len c =
    move len c |> ignore;
    ManagedMMap.get c.map (c.position - len) len
  ;;

  let write len b c =
    move len c |> ignore;
    ManagedMMap.set c.map (c.position - len) len (Array1.sub b 0 len);
    c
  ;;

  let seek offset c =
    c.position <- offset;
    c.length <- max c.length c.position;
    c
  ;;

  let len c = c.length
  let position c = c.position
  let to_bytes c = c.map.array

  let truncate c =
    Unix.ftruncate c.map.fd c.length;
    Unix.fsync c.map.fd;
    c.map <- ManagedMMap.of_file_descr c.map.fd
  ;;

  let close c = Unix.close c.map.fd
end
