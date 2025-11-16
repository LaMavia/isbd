open Bigarray

type big_bytes = (char, int8_unsigned_elt, c_layout) Array1.t

type stb = {
  mutable buffer : big_bytes;
  mutable position : int;
  mutable length : int;
}

type t = stb array

let read_bytes : big_bytes -> int -> int -> bytes =
 fun a i0 length ->
  let open Array1 in
  Bytes.init length (fun i -> get a (i0 + i))

let write_bytes : big_bytes -> int -> int -> bytes -> unit =
 fun a i0 length ->
  let open Array1 in
  Bytes.iteri (fun i b -> if i < length then set a (i0 + i) b else ())

let write_big_bytes : big_bytes -> int -> int -> big_bytes -> unit =
 fun a i0 len b ->
  let open Array1 in
  blit (sub b 0 len) (sub a i0 len)

let get_int64_be =
  let buffer = Bytes.make 8 '\000' in
  fun (a : big_bytes) (offset : int) ->
    for i = 0 to 7 do
      Array1.get a (offset + i) |> Bytes.set buffer i
    done;
    Bytes.get_int64_be buffer 0

let set_int64_be =
  let buffer = Bytes.make 8 '\000' in
  fun (a : big_bytes) (offset : int) (v : int64) ->
    Bytes.set_int64_be buffer 0 v;
    for i = 0 to 7 do
      Bytes.get buffer i |> Array1.set a (offset + i)
    done

let get_buffer (bfs : t) (i : int) = bfs.(i)
let create ~n ~len = Array.(make n () |> map (fun () -> Buffer.create len))
let create_bytes len = Array1.create Char c_layout len

let big_bytes_of_string s =
  let bts = String.to_bytes s in
  let len = Bytes.length bts in
  let a = create_bytes len in
  write_bytes a 0 len bts;
  a

let of_list bts =
  List.map (fun b -> { buffer = b; position = 0; length = Array1.dim b }) bts
  |> Array.of_list

(* let of_byte_list bts = *)
(*   bts *)
(*   |> List.map (fun b -> *)
(*          let len = Bytes.length b in *)
(*          let a = Array1.create Char c_layout len in *)
(*          write_bytes a 0 len b; *)
(*          a) *)
(*   |> of_list *)

let should_dump (threshold : int) (bfs : t) =
  let total_length = Array.fold_left (fun u a -> u + a.position) 0 bfs in
  total_length >= threshold

let free bfs =
  Array.iter Buffer.clear bfs;
  Gc.major ()
