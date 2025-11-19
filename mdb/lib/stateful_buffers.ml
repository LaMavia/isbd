open Bigarray

type big_bytes = (char, int8_unsigned_elt, c_layout) Array1.t

type stb =
  { mutable buffer : big_bytes
  ; mutable position : int
  ; mutable length : int
  }

type t = stb array

let read_bytes : big_bytes -> int -> int -> bytes =
  fun a i0 length ->
  let open Array1 in
  Bytes.init length (fun i ->
    (* Printf.eprintf "@[get] idx=%d / %d\n" (i0 + i) (dim a - 1); *)
    flush_all ();
    get a (i0 + i))
;;

let write_bytes : big_bytes -> int -> int -> bytes -> unit =
  fun a i0 length ->
  let open Array1 in
  Bytes.iteri (fun i b -> if i < length then set a (i0 + i) b else ())
;;

let write_big_bytes : big_bytes -> int -> int -> big_bytes -> unit =
  fun a i0 len b ->
  let open Array1 in
  blit (sub b 0 len) (sub a i0 len)
;;

let get_int64_be =
  let buffer = Bytes.make 8 '\000' in
  fun (a : big_bytes) (offset : int) ->
    for i = 0 to 7 do
      Array1.get a (offset + i) |> Bytes.set buffer i
    done;
    Bytes.get_int64_be buffer 0
;;

let set_int64_be =
  let buffer = Bytes.make 8 '\000' in
  fun (a : big_bytes) (offset : int) (v : int64) ->
    Bytes.set_int64_be buffer 0 v;
    for i = 0 to 7 do
      Bytes.get buffer i |> Array1.set a (offset + i)
    done
;;

let get_buffer (bfs : t) (i : int) = bfs.(i)

let set_buffer (bfs : t) (i : int) a =
  bfs.(i) <- { buffer = a; position = 0; length = Array1.dim a }
;;

let create_bytes len = Array1.create Char c_layout len

let create_stb len actual_len =
  { buffer = create_bytes actual_len; position = 0; length = len }
;;

let create ~n ~len ~actual_length = Array.init n (fun _ -> create_stb len actual_length)

let big_bytes_of_string s =
  let bts = String.to_bytes s in
  let len = Bytes.length bts in
  let a = create_bytes len in
  write_bytes a 0 len bts;
  a
;;

let of_list bts =
  List.map (fun b -> { buffer = b; position = 0; length = Array1.dim b }) bts
  |> Array.of_list
;;

let size bfs = Array.fold_left (fun u a -> u + a.position) 0 bfs

(* let of_byte_list bts = *)
(*   bts *)
(*   |> List.map (fun b -> *)
(*          let len = Bytes.length b in *)
(*          let a = Array1.create Char c_layout len in *)
(*          write_bytes a 0 len b; *)
(*          a) *)
(*   |> of_list *)

let should_dump (threshold : int) (bfs : t) = size bfs >= threshold
let are_empty bfs = Array.for_all (fun b -> b.position == 0) bfs
let empty = Array.iter (fun b -> b.position <- 0)

let blit_big_bytes src dist =
  let open Array1 in
  let len = src.position in
  let src_sub = sub src.buffer 0 len in
  let dist_sub = sub dist.buffer dist.position len in
  blit src_sub dist_sub;
  dist.position <- dist.position + len
;;

let blit src_bfs dist_bfs = Array.iter2 blit_big_bytes src_bfs dist_bfs

let free bfs =
  Array.iter Buffer.clear bfs;
  Gc.major ()
;;

let print_buffers label (bfs : t) =
  Printf.eprintf "%s:\n" label;
  Array.iteri
    (fun i b ->
       Printf.eprintf
         "%02d. length=%d, actual_length=%d, position=%d, "
         i
         b.length
         (Array1.dim b.buffer)
         b.position;
       Utils.Debugging.print_hex_bytes "bytes" b.buffer)
    bfs
;;
