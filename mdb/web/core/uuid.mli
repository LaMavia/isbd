type t

val t_of_yojson : Yojson.Safe.t -> t
val yojson_of_t : t -> Yojson.Safe.t
val v4 : unit -> t
val to_string : t -> string
