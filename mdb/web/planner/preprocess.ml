open Models

let rec preprocess_ce : ColumnExpression.t -> ColumnExpression.t =
  let open ColumnExpression in
  function
  | (`Literal _ | `ColumnReferenceExpression _) as e -> e
  | `ColumnarUnaryOperation { u_operator; u_operand } ->
    `ColumnarUnaryOperation { u_operator; u_operand = preprocess_ce u_operand }
  | `ColumnarBinaryOperation
      { b_operator = (`ADD | `MULTIPLY | `AND | `OR | `EQUAL | `NOT_EQUAL) as b_operator
      ; b_left_operand
      ; b_right_operand
      } ->
    let l, r = min b_left_operand b_right_operand, max b_left_operand b_right_operand in
    `ColumnarBinaryOperation
      { b_operator; b_left_operand = preprocess_ce l; b_right_operand = preprocess_ce r }
  | `ColumnarBinaryOperation { b_operator; b_left_operand; b_right_operand } ->
    `ColumnarBinaryOperation
      { b_operator
      ; b_left_operand = preprocess_ce b_left_operand
      ; b_right_operand = preprocess_ce b_right_operand
      }
  | `Function { function_name; arguments } ->
    `Function { function_name; arguments = List.map preprocess_ce arguments }
;;

let preprocess_select_query : SelectQuery.t -> SelectQuery.t =
  let open SelectQuery in
  fun q ->
    { column_clauses = List.map preprocess_ce q.column_clauses
    ; where_clause = Option.map preprocess_ce q.where_clause
    }
;;
