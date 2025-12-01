type id
type ('t, 'r, 's) t

val string_of_id : id -> string
val create : unit -> ('t, 'r, 's) t
val add_task : 't -> 's -> ('t, 'r, 's) t -> id
val pop_task : ('t, 'r, 's) t -> id * 't
val add_result : id -> 'r -> ('t, 'r, 's) t -> unit
val peek_result_opt : id -> ('t, 'r, 's) t -> 'r option
val pop_result_opt : id -> ('t, 'r, 's) t -> 'r option
val set_status : id -> 's -> ('t, 'r, 's) t -> unit
val peek_status_opt : id -> ('t, 'r, 's) t -> 's option
val peek_statuses : ('t, 'r, 's) t -> (id * 's) Seq.t

val show
  :  ?task:('t -> string) option
  -> ?result:('r -> string) option
  -> ?status:('s -> string) option
  -> ('t, 'r, 's) t
  -> string
