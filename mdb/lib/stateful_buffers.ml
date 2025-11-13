type stb = { buffer : bytes; mutable position : int }
type t = stb array

let get_buffer (bfs : t) (i : int) = bfs.(i)
let create ~n ~len = Array.(make n () |> map (fun () -> Buffer.create len))

let free bfs =
  Array.iter Buffer.clear bfs;
  Gc.major ()
