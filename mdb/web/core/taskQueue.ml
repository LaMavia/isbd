type id = Uuid.t [@@deriving yojson]

let string_of_id = Uuid.to_string
let id_of_string = Uuid.of_string
let uuid_of_id = Fun.id

exception ShouldStop

type ('t, 'r, 's) t =
  { queue : (id * 't) Queue.t
  ; results : (id, 'r) Hashtbl.t
  ; statuses : (id, 's) Hashtbl.t
  ; lock : Mutex.t
  ; nonempty : Condition.t
  ; mutable should_stop : bool
  }

let create () =
  { queue = Queue.create ()
  ; results = Hashtbl.create ~random:true 20
  ; statuses = Hashtbl.create ~random:true 20
  ; lock = Mutex.create ()
  ; nonempty = Condition.create ()
  ; should_stop = false
  }
;;

let with_tq q f = Mutex.protect q.lock (fun () -> f q)

let add_task task s q =
  with_tq q
  @@ fun q ->
  let id = Uuid.v4 ()
  and was_empty = Queue.is_empty q.queue in
  Queue.add (id, task) q.queue;
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
  let id, t = Queue.take q.queue in
  Hashtbl.replace q.statuses id s;
  id, t
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

let stop q =
  with_tq q
  @@ fun q ->
  q.should_stop <- true;
  Condition.broadcast q.nonempty
;;

let show ?(task = None) ?(result = None) ?(status = None) q =
  with_tq q
  @@ fun q ->
  let statuses_str =
    let len = Hashtbl.length q.statuses in
    Option.fold
      ~none:(Printf.sprintf "(%d)" len)
      ~some:(fun pp ->
        Printf.sprintf
          "(%d) {\n\t\t%s\n}"
          len
          (Hashtbl.to_seq q.statuses
           |> Seq.map (fun (id, t) ->
             Printf.sprintf "%s -> %s;" (Uuid.to_string id) (pp t))
           |> List.of_seq
           |> String.concat "\n\t"))
      status
  and queue_str =
    let len = Queue.length q.queue in
    Option.fold
      ~none:(Printf.sprintf "(%d)" len)
      ~some:(fun pp ->
        Printf.sprintf
          "(%d) {\n\t\t%s\n}"
          len
          (Queue.to_seq q.queue
           |> Seq.map (fun (id, t) ->
             Printf.sprintf "%s -> %s;" (Uuid.to_string id) (pp t))
           |> List.of_seq
           |> String.concat "\n\t"))
      task
  and results_str =
    let len = Hashtbl.length q.results in
    Option.fold
      ~none:(Printf.sprintf "(%d)" len)
      ~some:(fun pp ->
        Printf.sprintf
          "(%d) {\n\t\t%s\n}"
          len
          (Hashtbl.to_seq q.results
           |> Seq.map (fun (id, t) ->
             Printf.sprintf "%s -> %s;" (Uuid.to_string id) (pp t))
           |> List.of_seq
           |> String.concat "\n\t"))
      result
  in
  Printf.sprintf
    "Queue {\n\tstatuses=%s;\n\tqueue=%s;\n\tresults=%s;\n}"
    statuses_str
    queue_str
    results_str
;;
