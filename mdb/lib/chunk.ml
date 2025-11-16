type t = { data : Stateful_buffers.big_bytes; mutable pos : int }
type chunk_stream = t Seq.t

module Make (C : Cursor.CursorInterface) = struct
  (* let create src log_cols = *)
  (*   let open Utils.Mresult in *)
  (*   let* c = C.create src in *)
  (*   let aux () = Utils.Undefined.undefined in *)
  (*   Result.ok @@ Seq.of_dispenser aux *)
end
