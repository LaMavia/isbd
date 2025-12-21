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
  let state = Array.mapi (fun i _ -> next_seq_array streams i) streams in
  Seq.of_dispenser (fun () ->
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
    Printf.eprintf
      "[%s] next, M=%fMB\n"
      __FUNCTION__
      (Gc.allocated_bytes () /. 1024. /. 1024.);
    flush_all ();
    state.(i) <- next_seq_array streams i;
    min_val)
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
  let group_size = ref 0 in
  let chunk_descriptors =
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
    |> Seq.mapi (fun i s ->
      Printf.eprintf "[%s] sorting group %d\n" __FUNCTION__ i;
      flush_all ();
      s |> in_memory_sort cmp)
    |> Seq.mapi (fun i group_stream ->
      Printf.eprintf "[%s] writing group %d\n" __FUNCTION__ i;
      flush_all ();
      let temp_dist = Filename.temp_file "chunk" ".bin" in
      (try
         let cursor = Metastore.Store.write temp_dist cols group_stream in
         Metastore.Store.Internal.Cursor.close cursor
       with
       | _ ->
         Printf.eprintf "Forced twice in write\n";
         flush_all ());
      Printf.eprintf "Finished writing\n";
      flush_all ();
      temp_dist, Metastore.Store.Internal.Cursor.create temp_dist |> Result.get_ok)
    |> Seq.fold_left
         (fun u g ->
            try g :: u with
            | Seq.Forced_twice ->
              Printf.eprintf "Forced twice in append\n";
              flush_all ();
              u)
         []
    |> Array.of_list
  in
  Gc.major ();
  let chunk_streams =
    Array.map
      (fun (_temp_dist, cursor) -> Metastore.Store.read_cursor cursor)
      chunk_descriptors
  in
  Printf.eprintf "[%s] |chunk_streams|=%d\n" __FUNCTION__ (Array.length chunk_streams);
  flush_all ();
  let cleanup () =
    Array.iter
      (fun (temp_dist, cursor) ->
         Metastore.Store.Internal.Cursor.close cursor;
         Sys.remove temp_dist)
      chunk_descriptors
  in
  Fun.protect ~finally:cleanup @@ fun () -> k_way_merge cmp chunk_streams |> f
;;
