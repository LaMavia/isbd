open Bigarray

type big_bytes = (char, int8_unsigned_elt, c_layout) Array1.t

module Internal = struct
  external create_big_bytes
    :  (int[@untagged])
    -> big_bytes
    = "caml_create_big_bytes_byte" "caml_create_big_bytes"

  external free_big_bytes : big_bytes -> unit = "caml_free_big_bytes"
end

type stb =
  { mutable buffer : big_bytes
  ; mutable position : int
  ; mutable length : int
  }

type t = stb array

module DLS_Keys = struct
  open Domain

  let get_uint8 = DLS.new_key (fun () -> lazy (Bytes.make 1 '\000'))
  let get_int64be = DLS.new_key (fun () -> lazy (Bytes.make 8 '\000'))
  let set_int64be = DLS.new_key (fun () -> lazy (Bytes.make 8 '\000'))
end

let read_bytes : big_bytes -> int -> int -> bytes =
  fun a i0 length ->
  let open Array1 in
  Bytes.init length (fun i -> get a (i0 + i))
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

let get_uint8 =
  fun (a : big_bytes) (offset : int) ->
  let buffer = Domain.DLS.get DLS_Keys.get_uint8 |> Lazy.force in
  Array1.get a offset |> Bytes.set buffer 0;
  Bytes.get_uint8 buffer 0
;;

let get_int64_be =
  fun (a : big_bytes) (offset : int) ->
  let buffer = Domain.DLS.get DLS_Keys.get_int64be |> Lazy.force in
  for i = 0 to 7 do
    Array1.get a (offset + i) |> Bytes.set buffer i
  done;
  Bytes.get_int64_be buffer 0
;;

let set_int64_be =
  fun (a : big_bytes) (offset : int) (v : int64) ->
  let buffer = Domain.DLS.get DLS_Keys.set_int64be |> Lazy.force in
  Bytes.set_int64_be buffer 0 v;
  for i = 0 to 7 do
    Bytes.get buffer i |> Array1.set a (offset + i)
  done
;;

let create_bytes len = Internal.create_big_bytes len
let free_bytes ba = Internal.free_big_bytes ba

let create_stb len actual_len =
  { buffer = create_bytes actual_len; position = 0; length = len }
;;

let free_stb { buffer; _ } = free_bytes buffer
let create ~n ~len ~actual_length = Array.init n (fun _ -> create_stb len actual_length)
let free (bfs : t) = Array.iter (fun { buffer; _ } -> free_bytes buffer) bfs
let get_buffer (bfs : t) (i : int) = bfs.(i)

(** Frees the old buffer, and sets its slot to the new one *)
let set_buffer (bfs : t) (i : int) a =
  free_stb bfs.(i);
  bfs.(i) <- { buffer = a; position = 0; length = Array1.dim a }
;;

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

let copy_bytes a =
  let open Array1 in
  let a' = create_bytes (dim a) in
  blit a a';
  a'
;;

let blit src_bfs dist_bfs = Array.iter2 blit_big_bytes src_bfs dist_bfs

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
