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
  val to_bytes : t -> bytes
end

module StringCursor = struct
  type t = { mutable i : int; mutable buffer : bytes; mutable length : int }
  type src = string

  let create s =
    Result.ok { i = 0; buffer = Bytes.of_string s; length = String.length s }

  let len c = c.length

  let move di c =
    c.i <- c.i + di;
    c

  let seek i c =
    c.i <- i;
    c

  let read len c =
    let r = Bytes.sub c.buffer c.i len in
    move len c |> ignore;
    r

  let write len bs c =
    let blen = Bytes.length c.buffer in
    (if c.i + len >= blen then
       let added_len = c.i + len - blen in
       c.buffer <- Bytes.extend c.buffer 0 (added_len * 2));
    Bytes.blit bs 0 c.buffer c.i len;
    c.length <- max (c.i + len) c.length;
    move len c

  let position c = c.i
  let to_bytes c = Bytes.sub c.buffer 0 c.i
end
