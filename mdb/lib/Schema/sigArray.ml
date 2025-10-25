module ManagedMMap = struct
  let page_size = 16 * 1024
  let page_size_f = float_of_int page_size

  type t = {
    fd : Unix.file_descr;
    mutable size : int;
    mutable array :
      (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Genarray.t;
  }

  let map_file_descr fd =
    Unix.map_file fd Bigarray.Char Bigarray.C_layout true [| -1 |]

  let of_file_descr fd =
    let open Unix in
    { fd; array = map_file_descr fd; size = (fstat fd).st_size }

  let of_path path =
    let open Unix in
    openfile path [ O_RDWR ] 0o640 |> of_file_descr

  let get a offset length =
    let open Bigarray.Genarray in
    Bytes.init length (fun i -> get a.array [| offset + i |])

  let set a offset length =
    if offset + length >= a.size then (
      let extend_size =
        page_size
        * (int_of_float @@ ceil
          @@ (float_of_int (offset + length - a.size) /. page_size_f))
      in
      let len_written =
        Unix.write a.fd (Bytes.make extend_size '\000') a.size extend_size
      in
      a.size <- a.size + len_written;
      a.array <- map_file_descr a.fd;
      Gc.major ());
    let open Bigarray.Genarray in
    Bytes.iteri (fun i b ->
        if i < length then set a.array [| offset + i |] b else ())
end

type t = ManagedMMap.t

let read_bytes = ManagedMMap.get
let write_bytes = ManagedMMap.set
