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

let validate td_opt seen_columns e =
  let open Utils.Let.Res in
  let rec validate_ce
    :  ColumnExpression.t
    -> (ColumnExpression.expr_type, MultipleProblemsError.problem list) result
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
    | `REPLACE ->
      (match arguments with
       | [ arg_1; arg_2; arg_3 ] ->
         let* t1 = validate_ce arg_1
         and* t2 = validate_ce arg_2
         and* t3 = validate_ce arg_3 in
         (match t1, t2, t3 with
          | `Varchar, `Varchar, `Varchar -> Ok `Varchar
          | _ ->
            Error
              Exc.
                [ invalid_arguments
                    "REPLACE"
                    [ `Varchar; `Varchar; `Varchar ]
                    [ t1; t2; t3 ]
                ])
       | _ ->
         Error Exc.[ invalid_number_of_arguments "REPLACE" 3 (List.length arguments) ])
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
    let open Metastore.TableData in
    Hashtbl.replace seen_columns column_name ();
    match td_opt with
    | Some td when table_name = td.name ->
      (match Metastore.TableData.find_column_opt td column_name with
       | Some (_, ct) -> Ok (ColumnExpression.expr_type_of_lib ct)
       | None -> Error Exc.[ column_not_found table_name column_name ])
    | Some td -> Error Exc.[ multiple_tables_seen td.name table_name ]
    | None -> Error Exc.[ table_not_found table_name ]
  in
  validate_ce e
;;

let validate_where_clause seen_table seen_columns e =
  match validate seen_table seen_columns e with
  | Ok `Bool -> Ok `Bool
  | Ok t ->
    Error
      MultipleProblemsError.
        [ { error = "Invalid where clause type"
          ; context =
              Some
                (Printf.sprintf
                   "Expected the clause to be of type %s but got %s instead"
                   (ColumnExpression.string_of_expt_type `Bool)
                   (ColumnExpression.string_of_expt_type t))
          }
        ]
  | Error _ as err -> err
;;

let validate_order_by_clause SelectQuery.{ column_clauses; order_by_clause; _ } =
  let open OrderByExpression in
  let n_cols = List.length column_clauses in
  match order_by_clause with
  | None -> Ok ()
  | Some obc ->
    (match
       Array.find_mapi
         (fun i { column_index; _ } ->
            if column_index < 0 || column_index >= n_cols
            then Some (i, column_index)
            else None)
         obc
     with
     | None -> Ok ()
     | Some (i, coli) ->
       Error
         MultipleProblemsError.
           [ { error = "Invalid order by clause"
             ; context =
                 Some
                   (Printf.sprintf
                      "Invalid column index in order by clause %d. Expected column index \
                       0 ≤ %d < %d"
                      i
                      coli
                      n_cols)
             }
           ])
;;

let validate_limit_clause ({ limit } : LimitExpression.t) =
  if limit >= 0
  then Ok ()
  else
    Error
      MultipleProblemsError.
        [ { error = "Invalid limit clause"
          ; context =
              Some
                (Printf.sprintf
                   "Expected limit to be non-negative, but got %d instead"
                   limit)
          }
        ]
;;

let error_list_of_res res = Result.fold ~ok:(Fun.const []) ~error:Fun.id res

let validate_select_query td_opt (q : SelectQuery.t) =
  let seen_columns = Hashtbl.create ~random:true 0 in
  let select_res =
    Utils.Monad.mmap_result (validate td_opt seen_columns) q.column_clauses
  in
  let where_res = Option.map (validate_where_clause td_opt seen_columns) q.where_clause in
  let order_res = validate_order_by_clause q in
  let limit_res = Option.map validate_limit_clause q.limit_clause in
  match select_res, where_res, order_res with
  | Ok column_types, (Some (Ok `Bool) | None), Ok () -> Ok (q, column_types, seen_columns)
  | _ ->
    Error
      (List.concat
         [ error_list_of_res select_res
         ; Option.fold ~none:[] ~some:error_list_of_res where_res
         ; error_list_of_res order_res
         ; Option.fold ~none:[] ~some:error_list_of_res limit_res
         ])
;;

let get_copy_query_table ms (q : CopyQuery.t) =
  Metastore.Store.lookup_table_by_name q.destination_table_name ms
;;

let get_select_all_query_table ms (q : SelectAllQuery.t) =
  Metastore.Store.lookup_table_by_name q.table_name ms
;;

(* Returns the first referenced table that exists *)
let get_select_query_table ms (q : SelectQuery.t) =
  let open ColumnExpression in
  let rec visit_ce : ColumnExpression.t -> Metastore.TableData.t option = function
    | `ColumnReferenceExpression { table_name; _ } ->
      Metastore.Store.lookup_table_by_name table_name ms
    | `ColumnarBinaryOperation { b_left_operand; b_right_operand; _ } ->
      (match visit_ce b_left_operand with
       | None -> visit_ce b_right_operand
       | td -> td)
    | `ColumnarUnaryOperation { u_operand; _ } -> visit_ce u_operand
    | `Function { arguments; _ } -> List.fold_left fold_aux None arguments
    | `Literal _ -> None
  and fold_aux u ce =
    match u with
    | None -> visit_ce ce
    | Some td -> Some td
  in
  List.fold_left fold_aux (Option.bind q.where_clause visit_ce) q.column_clauses
;;

let get_query_table ms =
  let open QueryDefinition in
  function
  | QD_SelectQuery q -> get_select_query_table ms q
  | QD_CopyQuery q -> get_copy_query_table ms q
  | QD_SelectAllQuery q -> get_select_all_query_table ms q
;;
