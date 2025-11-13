module type CursorInterface = sig
  type t
  type src

  val create : src -> (t, string) result
  val read : int -> t -> bytes
  val write : int -> bytes -> t -> t
  val move : int -> t -> t
  val seek : int -> t -> t
  val len : t -> int
end

module StringCursor : sig
  include CursorInterface with type src := string
end = struct
  type t = { mutable i : int; buffer : bytes }

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
    Bytes.blit bs 0 c.buffer c.i len;
    move len c
end
