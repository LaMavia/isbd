type id = Uuid.t

type ('t, 'r, 's) t =
  { queue : (id * 't) Queue.t
  ; results : (id, 'r) Hashtbl.t
  ; statuses : (id, 's) Hashtbl.t
  ; lock : Mutex.t
  ; nonempty : Condition.t
  }

let create () =
  { queue = Queue.create ()
  ; results = Hashtbl.create ~random:true 20
  ; statuses = Hashtbl.create ~random:true 20
  ; lock = Mutex.create ()
  ; nonempty = Condition.create ()
  }
;;

let with_tq q f = Mutex.protect q.lock (fun () -> f q)

let add_task task s q =
  with_tq q
  @@ fun q ->
  let id = Uuid.v4 ()
  and was_empty = Queue.is_empty q.queue in
  Queue.add (id, task) q.queue;
  Hashtbl.add q.statuses id s;
  if was_empty then Condition.broadcast q.nonempty;
  id
;;

let pop_task q =
  with_tq q
  @@ fun q ->
  while Queue.is_empty q.queue do
    Condition.wait q.nonempty q.lock
  done;
  Queue.take q.queue
;;

let add_result id r q = with_tq q @@ fun q -> Hashtbl.add q.results id r
let peek_result_opt id q = with_tq q @@ fun q -> Hashtbl.find_opt q.results id

let pop_result_opt id q =
  with_tq q
  @@ fun q ->
  let open Lib.Utils.Mopt in
  let* r = Hashtbl.find_opt q.results id in
  Hashtbl.remove q.results id;
  Some r
;;

let set_status id s q = with_tq q @@ fun q -> Hashtbl.replace q.statuses id s
let peek_status_opt id q = with_tq q @@ fun q -> Hashtbl.find_opt q.statuses id
