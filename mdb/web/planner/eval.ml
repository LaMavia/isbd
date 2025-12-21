open Models
(* open Core *)

type t = (Lib.Data.Types.t, MultipleProblemsError.t) result

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
    | `NOT_EQUAL, l, r -> Ok (`DataBool (l != r))
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
       | Ok _ -> raise Core.QueryTask.(make_error [ { error = "_"; context = None } ]))
;;

let map
      eval
      (column_expressions : ColumnExpression.t list)
      (record : Lib.Data.data_record)
  =
  Utils.Monad.mmap_result (eval record) column_expressions
  |> Utils.Unwrap.result ~exc:Core.QueryTask.make_error
;;

let next_seq_array arr i =
  match arr.(i) () with
  | Seq.Nil -> None
  | Seq.Cons (v, rest) ->
    arr.(i) <- rest;
    Some (i, v)
;;

let k_way_merge cmp (streams : 'a Seq.t array) =
  let open Utils.Let.Opt in
  let streams = Array.map Seq.to_dispenser streams in
  let get_val i d = d () |> Option.map (fun v -> i, v) in
  let state = Array.mapi get_val streams in
  let aux () =
    let+ i, min_val =
      Array.fold_right
        (fun e u ->
           let ret =
             match e, u with
             | None, u -> u
             | (Some _ as l), None -> l
             | (Some (_, x) as l), Some (_, y) when cmp x y <= 0 -> l
             | _, u -> u
           in
           ret)
        state
        None
    in
    state.(i) <- get_val i streams.(i);
    min_val
  in
  Seq.of_dispenser aux
;;

let in_memory_sort cmp stream =
  let x = stream |> List.of_seq |> List.sort cmp |> List.to_seq in
  Gc.major ();
  x
;;

let group_eph_seq pred seq0 =
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
      Option.Some
        (Seq.cons
           seed
           (Seq.of_dispenser (fun () ->
              match !seq () with
              | Seq.Nil ->
                is_done := true;
                None
              | Seq.Cons (e, tail') when pred seed e ->
                seq := tail';
                Some e
              | Seq.Cons (e, tail') ->
                seq := Seq.cons e tail';
                None))))
;;

let make_temp_chunk_file () = Filename.temp_file "chunk" ".bin"

let group_chunks ~cols ~cmp ~est_size ~max_group_size stream =
  let group_size = ref 0 in
  group_eph_seq
    (fun _ q ->
       let q_size = est_size q in
       if !group_size + q_size > max_group_size
       then (
         group_size := q_size;
         false)
       else (
         group_size := !group_size + q_size;
         true))
    stream
  |> Seq.mapi (fun _i s ->
    Printf.eprintf "[%s] sorting group %d\n" __FUNCTION__ _i;
    flush stderr;
    s |> in_memory_sort cmp)
  |> Seq.mapi (fun _i group_stream ->
    Printf.eprintf "[%s] writing group %d\n" __FUNCTION__ _i;
    flush stderr;
    let temp_dist = make_temp_chunk_file () in
    let cursor = Metastore.Store.write temp_dist cols group_stream in
    Metastore.Store.Internal.Cursor.close cursor;
    (* Printf.eprintf "Finished writing\n"; *)
    (* flush_all (); *)
    temp_dist, Metastore.Store.Internal.Cursor.create temp_dist |> Result.get_ok)
  |> Array.of_seq
;;

(** [with_external_sort ~cols ~cmp ~est_size ~max_group_size f stream]

    1. splits [stream] of records with columns [cols] into groups of approximately [max_group_size] (estimated on per-record basis using [est_size]). The size can be exceeded by the estimated size of 1 record;

    2. sorts them in memory using [in_memory_sort cmp];

    3. writes them into temporary files on the disk;

    4. merges them using [k_way_merge cmp];

    5. evaluates [f] on the merged stream;

    6. closes the merged stream, and deletes the temporary files.


    [f] should consume the entire stream by the time it's done, because the resulting merged stream is ephemeral.
    *)
let with_external_sort ~cols ~cmp ~est_size ~max_group_size f stream =
  let chunk_descriptors = group_chunks ~cols ~cmp ~est_size ~max_group_size stream in
  let chunk_streams =
    Array.map
      (fun (_temp_dist, cursor) -> Metastore.Store.read_cursor cursor)
      chunk_descriptors
  in
  let cleanup () =
    Array.iter
      (fun (temp_dist, cursor) ->
         Metastore.Store.Internal.Cursor.close cursor;
         Sys.remove temp_dist)
      chunk_descriptors;
    Gc.full_major ()
  in
  Fun.protect ~finally:cleanup @@ fun () -> k_way_merge cmp chunk_streams |> f
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
    | None -> ()
    | Some (i : int) ->
      let a_temp_dist, a_cursor =
        desc_array.(2 * i)
        |> Utils.Unwrap.option
             ~message:
               (Printf.sprintf
                  "Invalid assumption: null left result %d for parent %d"
                  (2 * i)
                  i)
      and b_temp_dist, b_cursor =
        desc_array.((2 * i) + 1)
        |> Utils.Unwrap.option
             ~message:
               (Printf.sprintf
                  "Invalid assumption: null right result %d for parent %d"
                  ((2 * i) + 1)
                  i)
      in
      let own_temp_dist = make_temp_chunk_file () in
      let own_cursor =
        Fun.protect
          ~finally:(fun () ->
            Metastore.Store.Internal.Cursor.(
              close a_cursor;
              close b_cursor);
            Sys.(
              remove a_temp_dist;
              remove b_temp_dist))
          (fun () ->
             let a_stream = Metastore.Store.read_cursor a_cursor
             and b_stream = Metastore.Store.read_cursor b_cursor in
             try
               Seq.sorted_merge cmp a_stream b_stream
               |> Metastore.Store.write own_temp_dist cols
             with
             | exc ->
               Sys.remove own_temp_dist;
               raise exc)
      in
      Unix.fsync own_cursor.map.fd;
      desc_array.(i) <- Some (own_temp_dist, own_cursor);
      if Atomic.fetch_and_add pending_deps_array.(i / 2) (-1) = 1
      then Mutex.protect task_queue_lock (fun () -> Queue.add (i / 2) task_queue);
      aux ()
  in
  try aux () with
  | exc ->
    Printf.eprintf
      "[merge-worker %d] Exception: %s;\n  %s\n"
      worker_idx
      (Printexc.to_string exc)
      (Printexc.get_backtrace ());
    raise exc
;;

let with_parallel_external_sort ~n_workers ~cols ~cmp ~est_size ~max_group_size f stream =
  let chunk_descriptors = group_chunks ~cols ~cmp ~est_size ~max_group_size stream in
  let n_groups = Array.length chunk_descriptors in
  (* [pending_deps_array.(i)] is the number of pending child tasks for the task [i], for [i] > 0.

      [pending_deps_array.(i) = 0] implies [Option.is_some desc_array.(2*i)] and [Option.is_some desc_array.(2*i + 1)]
      *)
  let pending_deps_array = Array.init n_groups (fun _ -> Atomic.make 2) in
  (* [desc_array.(i) = (temp_dist, cursor)] stores the resulting [(temp_dist, cursor)] of the completed task [i].
      The parent task [i/2] is responsible for closing [cursor], removing [temp_dist], and setting [desc_array.(i)] to [None].

      Non-[None] elements will be cleaned up in case of an error.
      *)
  let desc_array = Array.make (2 * n_groups) None in
  let stderr_lock = Mutex.create () in
  let task_queue_lock = Mutex.create () in
  let task_queue = Queue.create () in
  (* Insert chunk results *)
  Array.iteri
    (fun i (temp_dist, cursor) ->
       let j = n_groups + i in
       desc_array.(j) <- Some (temp_dist, cursor);
       Atomic.decr pending_deps_array.(j / 2))
    chunk_descriptors;
  (* Initialise tasks *)
  Array.iteri
    (fun i pending_deps_atom ->
       if Atomic.get pending_deps_atom = 0 then Queue.add i task_queue)
    pending_deps_array;
  let workers =
    List.init n_workers (fun worker_idx ->
      Domain.spawn
        (worker_main
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
    desc_array.(1) |> Utils.Unwrap.option ~message:"Sorting failed?"
  in
  Fun.protect ~finally:(fun () ->
    Metastore.Store.Internal.Cursor.close result_cursor;
    Sys.remove result_temp_dist)
  @@ fun () -> result_cursor |> Metastore.Store.read_cursor |> f
;;

(* Fun.protect ~finally:cleanup @@ fun () -> k_way_merge cmp chunk_streams |> f *)

(* Atomic.compare_and_set *)
