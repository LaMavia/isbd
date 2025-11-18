type t = { data : Stateful_buffers.big_bytes; mutable pos : int }
type chunk_stream = t Seq.t

module Make : functor (C : Cursor.CursorInterface) -> sig
  val of_cursor : C.t -> t
  (*val dump_buffers : int array -> Stateful_buffers.t -> C.t -> unit*)
end =
functor
  (C : Cursor.CursorInterface)
  ->
  struct
    let of_cursor c = { data = C.to_bytes c; pos = C.position c }

    (*let dump_buffers (physical_lenghts : int array) (bfs : Stateful_buffers.t)
        (ch : C.t) =*)
  end
