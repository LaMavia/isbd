open Models
(* open Core *)

type t = (Lib.Data.Types.t, MultipleProblemsError.problem list) result

module Exc = struct
  open MultipleProblemsError

  let unimplemented ?(context = None) () = { error = "Unimplemented"; context }
end

let make_column_lookup (td : Metastore.TableData.t) =
  td.columns
  |> Array.to_seq
  |> Seq.mapi (fun id (cname, _) -> cname, id)
  |> Hashtbl.of_seq
;;

let column_filter_of_names
      (td : Metastore.TableData.t)
      (seen_columns : (string, unit) Hashtbl.t)
  =
  let lookup = make_column_lookup td in
  let column_idxs =
    Hashtbl.to_seq_keys seen_columns |> Seq.map (Hashtbl.find lookup) |> Array.of_seq
  in
  Array.sort Int.compare column_idxs;
  column_idxs
;;

let get_bool v =
  v
  |> Lib.Data.Types.get_bool
  |> MultipleProblemsError.of_string_result "unexpected casting error"
;;

let get_int64 v =
  v
  |> Lib.Data.Types.get_int64
  |> MultipleProblemsError.of_string_result "unexpected casting error"
;;

let get_varchar v =
  v
  |> Lib.Data.Types.get_varchar
  |> MultipleProblemsError.of_string_result "unexpected casting error"
;;

let eval_lit : ColumnExpression.literal -> Lib.Data.Types.t = function
  | `LitVarchar s -> `DataVarchar s
  | `LitInt i -> `DataInt i
  | `LitBool b -> `DataBool b
;;

let eval_unary : Lib.Data.Types.t -> ColumnExpression.unary_operation_name -> t =
  let open Utils.Let.Res in
  fun operand operator_name ->
    match operator_name with
    | `NOT ->
      let+ v = get_bool operand in
      `DataBool (not v)
    | `MINUS ->
      let+ v = get_int64 operand in
      `DataInt (Int64.neg v)
;;

let eval_binary
  : Lib.Data.Types.t -> Lib.Data.Types.t -> ColumnExpression.binary_operation_name -> t
  =
  fun left right operator_name ->
  try
    match operator_name, left, right with
    | `AND, `DataBool left, `DataBool right -> Ok (`DataBool (left && right))
    | `OR, `DataBool left, `DataBool right -> Ok (`DataBool (left || right))
    | `ADD, `DataInt left, `DataInt right -> Ok (`DataInt (Int64.add left right))
    | `SUBTRACT, `DataInt left, `DataInt right -> Ok (`DataInt (Int64.sub left right))
    | `MULTIPLY, `DataInt l, `DataInt r -> Ok (`DataInt (Int64.mul l r))
    | `DIVIDE, `DataInt l, `DataInt r -> Ok (`DataInt (Int64.div l r))
    | `EQUAL, l, r -> Ok (`DataBool (l = r))
    | `NOT_EQUAL, l, r -> Ok (`DataBool (l <> r))
    | `LESS_THAN, `DataInt l, `DataInt r -> Ok (`DataBool (l < r))
    | `LESS_EQUAL, `DataInt l, `DataInt r -> Ok (`DataBool (l <= r))
    | `GREATER_THAN, `DataInt l, `DataInt r -> Ok (`DataBool (l > r))
    | `GREATER_EQUAL, `DataInt l, `DataInt r -> Ok (`DataBool (l >= r))
    | op, l, r ->
      Error
        [ Exc.unimplemented
            ~context:
              (Printf.sprintf
                 "op=%s, l=%s, r=%s"
                 (Marshal.to_string op [])
                 (Marshal.to_string l [])
                 (Marshal.to_string r [])
               |> Option.some)
            ()
        ]
  with
  | Division_by_zero ->
    Error
      [ { error = "Division by zero"
        ; context =
            Printf.sprintf
              "%s / %s"
              (Lib.Data.Types.to_str left)
              (Lib.Data.Types.to_str right)
            |> Option.some
        }
      ]
;;

let eval_function : Lib.Data.Types.t list -> ColumnExpression.function_name -> t =
  fun args fname ->
  match fname, args with
  | `UPPER, [ `DataVarchar s ] -> Ok (`DataVarchar (String.uppercase_ascii s))
  | `LOWER, [ `DataVarchar s ] -> Ok (`DataVarchar (String.lowercase_ascii s))
  | `STRLEN, [ `DataVarchar s ] -> Ok (`DataInt (String.length s |> Int64.of_int))
  | `CONCAT, [ `DataVarchar l; `DataVarchar r ] -> Ok (`DataVarchar (String.cat l r))
  | fname, args ->
    Error
      [ Exc.unimplemented
          ~context:
            (Printf.sprintf
               "function=%s, args=%s"
               (Marshal.to_string fname [])
               (Marshal.to_string args [])
             |> Option.some)
          ()
      ]
;;

let make_eval_ce (td : Metastore.TableData.t option) =
  let open Utils.Let.Res in
  let column_lookup = Option.map make_column_lookup td in
  fun (record : Lib.Data.data_record) ->
    Cse.with_cse (fun eval e ->
      let open ColumnExpression in
      match e with
      | `Literal l -> Ok (eval_lit l)
      | `ColumnReferenceExpression { column_name; _ } ->
        Ok (Hashtbl.find (Option.get column_lookup) column_name |> Array.get record)
      | `ColumnarUnaryOperation { u_operator; u_operand } ->
        let* operand = eval u_operand in
        eval_unary operand u_operator
      | `ColumnarBinaryOperation { b_operator; b_left_operand; b_right_operand } ->
        let* left = eval b_left_operand
        and* right = eval b_right_operand in
        eval_binary left right b_operator
      | `Function { function_name; arguments } ->
        let* args = Utils.Monad.mmap_result eval arguments in
        eval_function args function_name)
;;

let filter eval (where_clause_opt : ColumnExpression.t option) =
  match where_clause_opt with
  | None -> Fun.const true
  | Some where_clause ->
    fun (record : Lib.Data.data_record) ->
      (match eval record where_clause with
       | Error err -> raise (Core.QueryTask.make_error err)
       | Ok (`DataBool (v : bool)) -> v
       | Ok v ->
         raise
           Core.QueryTask.(
             make_error
               [ { error = "Unexpected non-bool value in filter during execution"
                 ; context = Some (Printf.sprintf "value: %s" (Lib.Data.Types.to_str v))
                 }
               ]))
;;

let map
      eval
      (column_expressions : ColumnExpression.t list)
      (record : Lib.Data.data_record)
  =
  Utils.Monad.mmap_result (eval record) column_expressions
  |> Utils.Unwrap.result ~exc:Core.QueryTask.make_error
;;

let k_way_merge (type elem) cmp (streams : elem Seq.t array) =
  let open Utils.Let.Opt in
  let module ElemOrder = struct
    type t = int * elem

    let compare (_, x) (_, y) = cmp x y
  end
  in
  let module MinHeap = CCHeap.Make_from_compare (ElemOrder) in
  let streams = Array.map Seq.to_dispenser streams in
  let state = ref MinHeap.empty in
  Array.iteri
    (fun i d ->
       (* CCHeap.S.of_list is just as innefficient, 
          and nixpkgs doesn't provide ocaml@5.4,
          which has introduced a more efficient priority queue implementation - Pqueue 
          *)
       let- v = d () in
       state := MinHeap.insert (i, v) !state)
    streams;
  let aux () =
    let+ state', (i, min_val) = MinHeap.take !state in
    (match streams.(i) () with
     | Some e -> state := MinHeap.insert (i, e) state'
     | None -> state := state');
    min_val
  in
  Printf.eprintf "k-way-merging\n%!";
  Seq.of_dispenser aux
;;

let in_memory_sort cmp stream =
  let x = stream |> List.of_seq |> List.sort cmp |> List.to_seq in
  (* Gc.major (); *)
  x
;;

(** WARNING: Not thread-safe *)
let group_eph_seq weight max_weight seq0 =
  let is_done = ref false in
  let seq = ref seq0 in
  Seq.of_dispenser
  @@ fun () ->
  if !is_done
  then None
  else (
    match !seq () with
    | Seq.Nil -> None
    | Seq.Cons (seed, tail) ->
      seq := tail;
      let group_weight = ref (weight seed) in
      Option.Some
        (Seq.cons
           seed
           (Seq.of_dispenser (fun () ->
              match !seq () with
              | Seq.Nil ->
                is_done := true;
                None
              | Seq.Cons (e, tail') ->
                let e_weight = weight e in
                if !group_weight + e_weight <= max_weight
                then (
                  group_weight := !group_weight + e_weight;
                  seq := tail';
                  Some e)
                else (
                  seq := Seq.cons e tail';
                  None)))))
;;

let make_temp_chunk_file () = Filename.temp_file "chunk" ".bin"

module ExternalSortInternal = struct
  let sort_file_in_place ~temp_dist ~cursor ~cols ~cmp =
    cursor
    |> Metastore.Store.read_cursor
    |> List.of_seq
    |> List.sort cmp
    |> List.to_seq
    |> Metastore.Store.write temp_dist cols
    |> ignore
  ;;

  let worker_main
        ~worker_idx
        ~pending_deps_array
        ~cols
        ~cmp
        ~desc_array
        ~task_queue
        ~task_queue_lock
        ~stderr_lock:_
        ()
    =
    let rec aux () =
      match Mutex.protect task_queue_lock (fun () -> Queue.take_opt task_queue) with
      | None ->
        Printf.eprintf "[merge-worker %d] no longer needed, stopping\n" worker_idx;
        ()
      | Some (`Sort (i : int)) ->
        (* INFO: legacy task type from before parallel sorting was moved to [group_chunks].
           The initial queue tasks chould be changed, yet it's so inconsequential, I won't bother.
           *)
        if Atomic.fetch_and_add pending_deps_array.(i / 2) (-1) = 1
        then
          Mutex.protect task_queue_lock (fun () -> Queue.add (`Merge (i / 2)) task_queue);
        aux ()
      | Some (`Merge (i : int)) ->
        let left_temp_dist, left_cursor =
          desc_array.(2 * i)
          |> Utils.Unwrap.option
               ~message:
                 (Printf.sprintf
                    "Invalid assumption: null left result %d for parent %d%!"
                    (2 * i)
                    i)
        and right_temp_dist, right_cursor =
          desc_array.((2 * i) + 1)
          |> Utils.Unwrap.option
               ~message:
                 (Printf.sprintf
                    "Invalid assumption: null right result %d for parent %d"
                    ((2 * i) + 1)
                    i)
        in
        let own_temp_dist = make_temp_chunk_file () in
        Printf.eprintf
          "[merge-worker %d] merging %d, left:%s, right:%s into %s\n%!"
          worker_idx
          i
          left_temp_dist
          right_temp_dist
          own_temp_dist;
        let own_cursor =
          Fun.protect
            ~finally:(fun () ->
              Metastore.Store.Internal.Cursor.(
                close left_cursor;
                close right_cursor);
              Sys.(
                remove left_temp_dist;
                remove right_temp_dist))
            (fun () ->
               let open Metastore.Store in
               let a_stream = read_cursor left_cursor
               and b_stream = read_cursor right_cursor in
               try Seq.sorted_merge cmp a_stream b_stream |> write own_temp_dist cols with
               | exc ->
                 Sys.remove own_temp_dist;
                 raise exc)
        in
        desc_array.(i) <- Some (own_temp_dist, own_cursor);
        if Atomic.fetch_and_add pending_deps_array.(i / 2) (-1) = 1
        then
          Mutex.protect task_queue_lock (fun () -> Queue.add (`Merge (i / 2)) task_queue);
        aux ()
    in
    try aux () with
    | exc ->
      Printf.eprintf
        "[merge-worker %d] Exception: %s;\n  %s\n%!"
        worker_idx
        (Printexc.to_string exc)
        (Printexc.get_backtrace ());
      raise exc
  ;;
end

let group_chunks ~n_workers ~cmp ~cols ~est_size ~max_group_size stream =
  let should_die = ref false
  and empty_queue_cond = Condition.create ()
  and task_queue_lock = Mutex.create ()
  and task_queue = Queue.create () in
  let rec worker_aux ~worker_idx () =
    let task_opt =
      Mutex.protect task_queue_lock (fun () ->
        let task = ref None
        and will_stop = ref false in
        while not (!will_stop || Option.is_some !task) do
          match Queue.take_opt task_queue with
          | None ->
            if !should_die
            then will_stop := true
            else Condition.wait empty_queue_cond task_queue_lock
          | Some t -> task := Some t
        done;
        !task)
    in
    match task_opt with
    | None -> Printf.eprintf "[sort-worker %d] done\n%!" worker_idx
    | Some (temp_dist, elems) ->
      Printf.eprintf "[sort-worker %d] sorting file %s\n%!" worker_idx temp_dist;
      elems |> Array.sort cmp;
      elems |> Array.to_seq |> Metastore.Store.write temp_dist cols |> ignore;
      worker_aux ~worker_idx ()
  in
  let workers =
    List.init n_workers (fun worker_idx -> Domain.spawn @@ worker_aux ~worker_idx)
  in
  let result_array =
    group_eph_seq est_size max_group_size stream
    |> Seq.mapi (fun i group_stream ->
      Printf.eprintf "[%s] writing group %d\n%!" __FUNCTION__ i;
      let temp_dist = make_temp_chunk_file () in
      temp_dist, Array.of_seq group_stream)
    |> Seq.map (fun ((temp_dist, _) as task) ->
      Mutex.protect task_queue_lock (fun () ->
        Queue.add task task_queue;
        Condition.broadcast empty_queue_cond);
      temp_dist)
    |> Array.of_seq
  in
  should_die := true;
  Condition.broadcast empty_queue_cond;
  List.iter Domain.join workers;
  result_array
;;

let with_external_sort ~chunk_descriptors ~cmp f =
  let open Utils.Ops in
  let chunk_streams = Array.map (snd @> Metastore.Store.read_cursor) chunk_descriptors in
  let cleanup () =
    Array.iter
      (fun (temp_dist, cursor) ->
         Metastore.Store.Internal.Cursor.close cursor;
         Sys.remove temp_dist)
      chunk_descriptors
  in
  Fun.protect ~finally:cleanup @@ fun () -> k_way_merge cmp chunk_streams |> f
;;

let with_parallel_external_sort ~chunk_descriptors ~n_workers ~cols ~cmp f =
  (* let chunk_descriptors = group_chunks ~cols ~est_size ~max_group_size stream in *)
  let n_groups = Array.length chunk_descriptors in
  (* [pending_deps_array.(i)] is the number of pending child tasks for the task [i], for [i] > 0.

      [pending_deps_array.(i) = 0] implies [Option.is_some desc_array.(2*i)] and [Option.is_some desc_array.(2*i + 1)]
      *)
  let pending_deps_array =
    Array.init (2 * n_groups) (fun i -> Atomic.make (if i < n_groups then 2 else 0))
  in
  (* [desc_array.(i) = (temp_dist, cursor)] stores the resulting [(temp_dist, cursor)] of the completed task [i].
      The parent task [i/2] is responsible for closing [cursor], removing [temp_dist], and setting [desc_array.(i)] to [None].

      Non-[None] elements will be cleaned up in case of an error.
      *)
  let desc_array = Array.make (2 * n_groups) None in
  let stderr_lock = Mutex.create () in
  let task_queue_lock = Mutex.create () in
  let task_queue : [ `Sort of int | `Merge of int ] Queue.t = Queue.create () in
  (* Insert chunk results *)
  Array.iteri
    (fun i (temp_dist, cursor) ->
       let j = n_groups + i in
       desc_array.(j) <- Some (temp_dist, cursor))
    chunk_descriptors;
  (* Initialise tasks *)
  Array.iteri
    (fun i pending_deps_atom ->
       if Atomic.get pending_deps_atom = 0 then Queue.add (`Sort i) task_queue)
    pending_deps_array;
  let workers =
    List.init n_workers (fun worker_idx ->
      Domain.spawn
        (ExternalSortInternal.worker_main
           ~worker_idx
           ~pending_deps_array
           ~cmp
           ~cols
           ~desc_array
           ~task_queue
           ~stderr_lock
           ~task_queue_lock))
  in
  List.iter Domain.join workers;
  let result_temp_dist, result_cursor =
    desc_array.(1)
    |> Utils.Unwrap.option ~message:"Sorting failed? This really shouldn't happen"
  in
  Fun.protect ~finally:(fun () ->
    Metastore.Store.Internal.Cursor.close result_cursor;
    Sys.remove result_temp_dist)
  @@ fun () -> result_cursor |> Metastore.Store.read_cursor |> f
;;

let with_generic_external_sort
      ~n_workers
      ~cols
      ~cmp
      ~est_size
      ~max_group_size
      ~k_way_threshold
      f
      stream
  =
  let chunk_descriptors =
    group_chunks ~cols ~est_size ~max_group_size ~n_workers:2 ~cmp stream
    |> Array.map (fun f -> f, Metastore.Store.Internal.Cursor.create f |> Result.get_ok)
  in
  let n_groups = Array.length chunk_descriptors in
  if n_groups <= k_way_threshold
  then (
    Printf.eprintf "[%s] %d ≤ %d: k-way sort\n" __FUNCTION__ n_groups k_way_threshold;
    flush stderr;
    with_external_sort ~chunk_descriptors ~cmp f)
  else (
    Printf.eprintf
      "[%s] %d > %d: paraller log sort\n"
      __FUNCTION__
      n_groups
      k_way_threshold;
    flush stderr;
    with_parallel_external_sort ~chunk_descriptors ~n_workers ~cols ~cmp f)
;;

let with_sort ~ms:_ ~td ~order_by_clause_opt f stream =
  match order_by_clause_opt with
  | Some order_by_clause ->
    let asc =
      Array.map
        (fun OrderByExpression.{ ascending; _ } -> if ascending then 1 else -1)
        order_by_clause
    in
    (* let cmp_length = Array.length order_by_clause in *)
    let cmp a b =
      order_by_clause
      |> Array.find_mapi (fun i OrderByExpression.{ column_index = ci; _ } ->
        let cmp_v = asc.(i) * Lib.Data.Types.cmp a.(ci) b.(ci) in
        if cmp_v <> 0 then Some cmp_v else None)
      |> Option.value ~default:0
    in
    stream
    |> with_generic_external_sort
         ~n_workers:Config.merge_workers
         ~cols:Metastore.TableData.(td.columns)
         ~cmp
         ~est_size:Lib.Data.approx_record_size
         ~max_group_size:Metastore.Const.buffer_size
         ~k_way_threshold:Config.max_k_merge
         f
  | None -> f stream
;;

let limit ~limit_clause_opt stream =
  match limit_clause_opt with
  | Some LimitExpression.{ limit } -> Seq.take limit stream
  | None -> stream
;;
