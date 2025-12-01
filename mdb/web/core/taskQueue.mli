type id
type ('t, 'r, 's) t

val string_of_id : id -> string

(** [create ()] creates an empty task queue.*)
val create : unit -> ('t, 'r, 's) t

(** [add_task task status tq] add task [task] with status [status] to task queue [tq]. Returns the new task id*)
val add_task : 't -> 's -> ('t, 'r, 's) t -> id

(** [pop_task status tq] pops the oldest task from the task queue [tq], setting its status to [status]. 
    Returns the pair [(task_id, task)].

    If [tq] is empty, hangs until a new task has been submitted via [add_task task status tq].*)
val pop_task : 's -> ('t, 'r, 's) t -> id * 't

(** [add_result id result status tq] adds the result [result] for the task with task id [id], and sets its status to [s].
    It doesn't check if the task already exists.
    *)
val add_result : id -> 'r -> 's -> ('t, 'r, 's) t -> unit

(** [peek_result_opt id tq] gets the result and status of the task with task id [id] in [tq]. 

    If status is [None], the task has either never been submitted or has since been removed using [pop_result_opt id tq].
    
    If result is [None], and status is not [None], the task hasn't been completed yet.
    *)
val peek_result_opt : id -> ('t, 'r, 's) t -> 'r option * 's option

(** [pop_result_opt id tq] pops the pair [(task, status)] from the queue. Please reference [peek_result_opt].

      Returns [None] if the task has not been found or hasn't been completed yet.
   *)
val pop_result_opt : id -> ('t, 'r, 's) t -> ('r * 's) option

(** [peek_statuses tq] returns all the task statuses in [tq].*)
val peek_statuses : ('t, 'r, 's) t -> (id * 's) Seq.t

val show
  :  ?task:('t -> string) option
  -> ?result:('r -> string) option
  -> ?status:('s -> string) option
  -> ('t, 'r, 's) t
  -> string
