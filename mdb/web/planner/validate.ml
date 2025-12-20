open Models

module Exc = struct
  open MultipleProblemsError

  let invalid_arguments f expected got =
    { error = "Invalid arguments"
    ; context =
        Some
          (Printf.sprintf
             "Expected (%s) for %s but got (%s) instead"
             (String.concat ", " (List.map ColumnExpression.string_of_expt_type expected))
             f
             (String.concat ", " (List.map ColumnExpression.string_of_expt_type got)))
    }
  ;;

  let invalid_number_of_arguments f expected got =
    { error = "Invalid number of arguments"
    ; context =
        Some
          (Printf.sprintf
             "Expected %d arguments for %s but got %d instead"
             expected
             f
             got)
    }
  ;;

  let types_not_equal f lt rt =
    { error = "Types don't match"
    ; context =
        Some
          (Printf.sprintf
             "Expected arguments of %s to be of the same type, but %s ≠ %s"
             f
             (ColumnExpression.string_of_expt_type lt)
             (ColumnExpression.string_of_expt_type rt))
    }
  ;;

  let table_not_found t =
    { error = "Table not found"
    ; context = Some (Printf.sprintf "Table «%s» not found" t)
    }
  ;;

  let column_not_found t c =
    { error = "Column not found"
    ; context = Some (Printf.sprintf "Column «%s.%s» not found" t c)
    }
  ;;

  let multiple_tables_seen t1 t2 =
    { error = "Multiple tables"
    ; context =
        Some
          (Printf.sprintf "Encountered «%s» but «%s» had already beed encountered" t2 t1)
    }
  ;;
end

(** requires a lock on [ms] *)
let validate ms seen_table e =
  let open Utils.Let.Res in
  let rec validate_ce
    : ColumnExpression.t -> (ColumnExpression.expr_type, MultipleProblemsError.t) result
    = function
    | `Literal l -> validate_lit l
    | `Function f -> validate_function f
    | `ColumnarUnaryOperation ue -> validate_ue ue
    | `ColumnarBinaryOperation be -> validate_be be
    | `ColumnReferenceExpression rf -> validate_colref rf
  and validate_lit = function
    | `LitVarchar _ -> Ok `Varchar
    | `LitInt _ -> Ok `Int
    | `LitBool _ -> Ok `Bool
  and validate_function { function_name; arguments } =
    match function_name with
    | `CONCAT ->
      (match arguments with
       | [ arg_1; arg_2 ] ->
         let* t1 = validate_ce arg_1
         and* t2 = validate_ce arg_2 in
         (match t1, t2 with
          | `Varchar, `Varchar -> Ok `Varchar
          | _ ->
            Error Exc.[ invalid_arguments "CONCAT" [ `Varchar; `Varchar ] [ t1; t2 ] ])
       | _ -> Error Exc.[ invalid_number_of_arguments "CONCAT" 2 (List.length arguments) ])
    | `LOWER ->
      (match arguments with
       | [ arg ] ->
         let* t = validate_ce arg in
         (match t with
          | `Varchar -> Ok `Varchar
          | _ -> Error Exc.[ invalid_arguments "LOWER" [ `Varchar ] [ t ] ])
       | _ -> Error Exc.[ invalid_number_of_arguments "LOWER" 1 (List.length arguments) ])
    | `UPPER ->
      (match arguments with
       | [ arg ] ->
         let* t = validate_ce arg in
         (match t with
          | `Varchar -> Ok `Varchar
          | _ -> Error Exc.[ invalid_arguments "UPPER" [ `Varchar ] [ t ] ])
       | _ -> Error Exc.[ invalid_number_of_arguments "UPPER" 1 (List.length arguments) ])
    | `STRLEN ->
      (match arguments with
       | [ arg ] ->
         let* t = validate_ce arg in
         (match t with
          | `Varchar -> Ok `Int
          | _ -> Error Exc.[ invalid_arguments "STRLEN" [ `Varchar ] [ t ] ])
       | _ -> Error Exc.[ invalid_number_of_arguments "STRLEN" 1 (List.length arguments) ])
  and validate_ue { u_operator; u_operand } =
    let* t = validate_ce u_operand in
    match u_operator, t with
    | `NOT, `Bool -> Ok `Bool
    | `NOT, t -> Error Exc.[ invalid_arguments "NOT" [ `Bool ] [ t ] ]
    | `MINUS, `Int -> Ok `Int
    | `MINUS, t -> Error Exc.[ invalid_arguments "MINUS" [ `Int ] [ t ] ]
  and validate_be { b_operator; b_left_operand; b_right_operand } =
    let* lt = validate_ce b_left_operand
    and* rt = validate_ce b_right_operand in
    let operator_name =
      b_operator
      |> [%yojson_of: ColumnExpression.binary_operation_name]
      |> Yojson.Safe.to_string
    in
    match b_operator with
    | `ADD | `SUBTRACT | `MULTIPLY | `DIVIDE ->
      (match lt, rt with
       | `Int, `Int -> Ok `Int
       | lt, rt -> Error Exc.[ invalid_arguments operator_name [ `Int; `Int ] [ lt; rt ] ])
    | `GREATER_EQUAL | `GREATER_THAN | `LESS_EQUAL | `LESS_THAN | `EQUAL | `NOT_EQUAL ->
      if lt = rt then Ok `Bool else Error Exc.[ types_not_equal operator_name lt rt ]
    | `AND | `OR ->
      (match lt, rt with
       | `Bool, `Bool -> Ok `Bool
       | lt, rt ->
         Error Exc.[ invalid_arguments operator_name [ `Bool; `Bool ] [ lt; rt ] ])
  and validate_colref ColumnExpression.{ table_name; column_name } =
    match Metastore.Store.lookup_table_by_name table_name ms with
    | Some td ->
      (match Metastore.TableData.find_column_opt td column_name with
       | Some (_, ct) ->
         let* () =
           match !seen_table with
           | None ->
             seen_table := Some table_name;
             Ok ()
           | Some tname when tname = table_name -> Ok ()
           | Some tname -> Error Exc.[ multiple_tables_seen tname table_name ]
         in
         Ok (ColumnExpression.expr_type_of_lib ct)
       | None -> Error Exc.[ column_not_found table_name column_name ])
    | None -> Error Exc.[ table_not_found table_name ]
  in
  validate_ce e
;;

let validate_select_query ms (q : SelectQuery.t) =
  let seen_table = ref None in
  let select_res = Utils.Monad.mmap_result (validate ms seen_table) q.column_clauses in
  let where_res = Option.map (validate ms seen_table) q.where_clause in
  match select_res, where_res with
  | Ok _, (Some (Ok _) | None) -> Ok q
  | _ ->
    Error
      (List.append
         (Result.fold ~ok:(Fun.const []) ~error:Fun.id select_res)
         (Option.fold
            ~none:[]
            ~some:(Result.fold ~ok:(Fun.const []) ~error:Fun.id)
            where_res))
;;
