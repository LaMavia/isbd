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
