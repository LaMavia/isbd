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
  type t = { mutable i : int; mutable buffer : big_bytes; mutable length : int }
  type src = string

  let create s =
    Result.ok
      {
        i = 0;
        buffer = Stateful_buffers.big_bytes_of_string s;
        length = String.length s;
      }

  let len c = c.length

  let move di c =
    c.i <- c.i + di;
    c

  let seek i c =
    c.i <- i;
    c

  let read len c =
    let r = Array1.sub c.buffer c.i len in
    move len c |> ignore;
    r

  let write len bs c =
    let blen = c.length in
    if c.i + len >= blen then (
      let added_len = c.i + len - blen in
      let a' = create_bytes (blen + (added_len * 2)) in
      write_big_bytes a' 0 c.length c.buffer;
      c.buffer <- a');
    write_big_bytes c.buffer c.i len bs;
    c.length <- max (c.i + len) c.length;
    move len c

  let position c = c.i
  let to_bytes c = Array1.sub c.buffer 0 c.i
end

(* module BufferCursor : CursorInterface with type src := big_bytes = struct *)
(*   type t = { mutable i : int; mutable buffer : big_bytes; mutable length : int } *)
(**)
(*   let create b = Result.ok { i = 0; buffer = b; length = Array1.dim b } *)
(*   let len c = c.length *)
(**)
(*   let move di c = *)
(*     c.i <- c.i + di; *)
(*     c *)
(**)
(*   let seek i c = c.i <- i; c *)
(**)
(*   let read len c = *)
(*     let r = Bytes.init len (fun i -> Array1.get c.buffer (c.i + i) ) in *)
(**)
(* end *)
