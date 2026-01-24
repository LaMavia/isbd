type id = Uuid.t [@@deriving yojson]
type ticket = int

let string_of_id = Uuid.to_string
let id_of_string = Uuid.of_string
let uuid_of_id = Fun.id

exception ShouldStop

module TaskOrder = struct
  type t = id * ticket

  let leq (_, a) (_, b) = a <= b
end

module TicketHeap = CCHeap.Make (TaskOrder)

type ('t, 'r, 's) t =
  { queue : (id * ticket) Queue.t
  ; results : (id, 'r) Hashtbl.t
  ; statuses : (id, 's) Hashtbl.t
  ; tasks : (id, 't) Hashtbl.t
  ; lock : Mutex.t
  ; nonempty : Condition.t
  ; mutable should_stop : bool
  ; next_available_ticket : ticket Atomic.t
    (* ; mutable current_ticket : ticket *)
    (* ; my_turn_cond : Condition.t *)
    (* ; my_turn_lock : Mutex.t *)
  }

let create () =
  { queue = Queue.create ()
  ; results = Hashtbl.create ~random:true 20
  ; statuses = Hashtbl.create ~random:true 20
  ; tasks = Hashtbl.create ~random:true 20
  ; lock = Mutex.create ()
  ; nonempty = Condition.create ()
  ; should_stop = false
  ; next_available_ticket =
      Atomic.make 0
      (* ; current_ticket = 0 *)
      (* ; my_turn_cond = Condition.create () *)
      (* ; my_turn_lock = Mutex.create () *)
  }
;;

let with_tq q f = Mutex.protect q.lock (fun () -> f q)
let get_ticket q = Atomic.fetch_and_add q.next_available_ticket 1
let with_ticket _q _my_ticket f = f ()
(* Mutex.protect q.my_turn_lock *)
(* @@ fun () -> *)
(* Printf.eprintf *)
(*   "[%s] my_ticket=%d, current_ticket=%d\n%!" *)
(*   __FUNCTION__ *)
(*   my_ticket *)
(*   q.current_ticket; *)
(* while my_ticket > q.current_ticket && not q.should_stop do *)
(*   Condition.wait q.my_turn_cond q.my_turn_lock *)
(* done; *)
(* Fun.protect *)
(*   ~finally:(fun () -> *)
(*     q.current_ticket <- q.current_ticket + 1; *)
(*     Condition.broadcast q.my_turn_cond) *)
(*   (fun () -> *)
(*      if q.should_stop then raise ShouldStop; *)
(*      f ()) *)

let add_task task ticket s q =
  with_tq q
  @@ fun q ->
  let id = Uuid.v4 ()
  and was_empty = Queue.is_empty q.queue in
  Queue.add (id, ticket) q.queue;
  Hashtbl.replace q.tasks id task;
  Hashtbl.replace q.statuses id s;
  if was_empty then Condition.broadcast q.nonempty;
  id
;;

let pop_task s q =
  with_tq q
  @@ fun q ->
  if q.should_stop then raise ShouldStop;
  while Queue.is_empty q.queue do
    Condition.wait q.nonempty q.lock;
    if q.should_stop then raise ShouldStop
  done;
  let id, ticket = Queue.take q.queue in
  let t = Hashtbl.find q.tasks id in
  Hashtbl.replace q.statuses id s;
  id, ticket, t
;;

let add_result id r s q =
  with_tq q
  @@ fun q ->
  Hashtbl.replace q.results id r;
  Hashtbl.replace q.statuses id s
;;

let peek_result_opt id q =
  with_tq q @@ fun q -> Hashtbl.find_opt q.results id, Hashtbl.find_opt q.statuses id
;;

let pop_result_opt id q =
  with_tq q
  @@ fun q ->
  let open Lib.LibUtils.Mopt in
  let* r = Hashtbl.find_opt q.results id in
  let* s = Hashtbl.find_opt q.statuses id in
  Hashtbl.remove q.results id;
  Hashtbl.remove q.statuses id;
  Some (r, s)
;;

let peek_statuses q = with_tq q @@ fun q -> Hashtbl.to_seq q.statuses

let set_status id status q =
  with_tq q
  @@ fun q ->
  if Hashtbl.mem q.statuses id
  then Hashtbl.replace q.statuses id status
  else
    raise
      (Invalid_argument (Printf.sprintf "task with id=%s doesn't exist" (string_of_id id)))
;;

let peek_task_definition_exc id q = with_tq q @@ fun q -> Hashtbl.find q.tasks id

let stop q =
  with_tq q
  @@ fun q ->
  q.should_stop <- true;
  Condition.broadcast q.nonempty
;;
