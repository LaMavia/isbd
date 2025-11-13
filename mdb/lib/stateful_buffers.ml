type stb = { mutable buffer : bytes; mutable position : int }
type t = stb array

let get_buffer (bfs : t) (i : int) = bfs.(i)
let create ~n ~len = Array.(make n () |> map (fun () -> Buffer.create len))

let of_list bts =
  List.map (fun b -> { buffer = b; position = 0 }) bts |> Array.of_list

let should_dump (threshold : int) (bfs : t) =
  let total_length = Array.fold_left (fun u a -> u + a.position) 0 bfs in
  total_length >= threshold

let free bfs =
  Array.iter Buffer.clear bfs;
  Gc.major ()
