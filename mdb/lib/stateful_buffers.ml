open Bigarray

type big_bytes = (char, int8_unsigned_elt, c_layout) Array1.t

module External = struct
  external create_big_bytes
    :  (int[@untagged])
    -> big_bytes
    = "caml_create_big_bytes_byte" "caml_create_big_bytes"

  external free_big_bytes : big_bytes -> unit = "caml_free_big_bytes"

  external get_uint8
    :  big_bytes
    -> (int[@untagged])
    -> (int[@untagged])
    = "caml_get_uint8_byte" "caml_get_uint8"

  external get_int64_be
    :  big_bytes
    -> (int[@untagged])
    -> (int64[@unboxed])
    = "caml_get_int64_be_byte" "caml_get_int64_be"

  external set_int64_be
    :  big_bytes
    -> (int[@untagged])
    -> (int64[@unboxed])
    -> unit
    = "caml_set_int64_be_byte" "caml_set_int64_be"

  external encode_vle
    :  big_bytes
    -> big_bytes
    -> (int[@untagged])
    -> (int[@untagged])
    -> (int[@untagged])
    = "caml_encode_vle_byte" "caml_encode_vle"
end

type stb =
  { mutable buffer : big_bytes
  ; mutable position : int
  ; mutable length : int
  }

type t = stb array

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

let get_uint8 = External.get_uint8
let get_int64_be = External.get_int64_be
let set_int64_be = External.set_int64_be
let create_bytes len = External.create_big_bytes len
let free_bytes ba = External.free_big_bytes ba

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

module LZ4_Storage : LZ4.S with type storage := big_bytes = struct
  let compress_into input output = LZ4.Bigbytes.compress_into input output

  let compress input =
    let length = LZ4.compress_bound (Array1.dim input) in
    let output = create_bytes length in
    let length' = compress_into input output in
    if length' <> length then Array1.sub output 0 length' else output
  ;;

  let decompress_into input output = LZ4.Bigbytes.decompress_into input output

  let decompress ~length input =
    if length < 0 then invalid_arg "LZ4.decompress: negative length";
    let output = create_bytes length in
    let length' = decompress_into input output in
    if length' <> length then Array1.sub output 0 length' else output
  ;;
end
