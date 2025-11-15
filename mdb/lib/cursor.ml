module type CursorInterface = sig
  type t
  type src

  val create : src -> (t, string) result
  val read : int -> t -> bytes
  val write : int -> bytes -> t -> t
  val move : int -> t -> t
  val seek : int -> t -> t
  val len : t -> int
  val position : t -> int
end

module StringCursor : sig
  include CursorInterface with type src := string
end = struct
  type t = { mutable i : int; mutable buffer : bytes }

  let create s = Result.ok { i = 0; buffer = Bytes.of_string s }
  let len c = Bytes.length c.buffer

  let move di c =
    c.i <- c.i + di;
    c

  let seek i c =
    c.i <- i;
    c

  let read len c =
    let r = Bytes.sub c.buffer c.i len in
    c.i <- c.i + len;
    r

  let write len bs c =
    let blen = Bytes.length c.buffer in
    if c.i + len >= blen then
      c.buffer <- Bytes.extend c.buffer 0 ((c.i + len - blen) * 2);
    Bytes.blit bs 0 c.buffer c.i len;
    move len c

  let position c = c.i
end
