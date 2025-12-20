open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type expr_type =
  [ `Int
  | `Varchar
  | `Bool
  ]

let string_of_expt_type : expr_type -> string = function
  | `Bool -> "BOOL"
  | `Int -> "INT64"
  | `Varchar -> "VARCHAR"
;;

let expr_type_of_lib : Lib.Column.col -> expr_type = function
  | `ColInt -> `Int
  | `ColVarchar -> `Varchar
;;

let lib_of_expr_type_exc : expr_type -> Lib.Column.col = function
  | `Int -> `ColInt
  | `Varchar -> `ColVarchar
  | `Bool -> raise (Invalid_argument "Can't cast Bool to data")
;;

type column_reference_expression =
  { table_name : string [@key "tableName"]
  ; column_name : string [@key "columnName"]
  }
[@@deriving yojson]

type literal =
  [ `LitInt of int64
  | `LitVarchar of string
  | `LitBool of bool
  ]

let yojson_of_literal : literal -> Yojson.Safe.t = function
  | `LitInt i -> `Intlit (Int64.to_string i)
  | `LitVarchar s -> `String s
  | `LitBool b -> `Bool b
;;

let literal_of_yojson : Yojson.Safe.t -> literal = function
  | `Intlit is -> `LitInt (Int64.of_string is)
  | `Int i -> `LitInt (Int64.of_int i)
  | `String s -> `LitVarchar s
  | `Bool b -> `LitBool b
  | j ->
    Yojson.json_error
    @@ Printf.sprintf
         "Expected a (string|int|bool) but got «%s»"
         (Yojson.Safe.to_string j)
;;

type function_name =
  [ `STRLEN
  | `CONCAT
  | `UPPER
  | `LOWER
  ]

let function_name_of_yojson : Yojson.Safe.t -> function_name = function
  | `String "STRLEN" -> `STRLEN
  | `String "CONCAT" -> `CONCAT
  | `String "UPPER" -> `UPPER
  | `String "LOWER" -> `LOWER
  | j ->
    Yojson.json_error
    @@ Printf.sprintf
         "Expected a (STRLEN|CONCAT|UPPER|LOWER) but got «%s»"
         (Yojson.Safe.to_string j)
;;

let yojson_of_function_name : function_name -> Yojson.Safe.t = function
  | `STRLEN -> `String "STRLEN"
  | `CONCAT -> `String "CONCAT"
  | `UPPER -> `String "UPPER"
  | `LOWER -> `String "LOWER"
;;

type binary_operation_name =
  [ `ADD
  | `SUBTRACT
  | `MULTIPLY
  | `DIVIDE
  | `AND
  | `OR
  | `EQUAL
  | `NOT_EQUAL
  | `LESS_THAN
  | `LESS_EQUAL
  | `GREATER_THAN
  | `GREATER_EQUAL
  ]

let yojson_of_binary_operation_name : binary_operation_name -> Yojson.Safe.t = function
  | `ADD -> `String "ADD"
  | `SUBTRACT -> `String "SUBTRACT"
  | `MULTIPLY -> `String "MULTIPLY"
  | `DIVIDE -> `String "DIVIDE"
  | `AND -> `String "AND"
  | `OR -> `String "OR"
  | `EQUAL -> `String "EQUAL"
  | `NOT_EQUAL -> `String "NOT_EQUAL"
  | `LESS_THAN -> `String "LESS_THAN"
  | `LESS_EQUAL -> `String "LESS_EQUAL"
  | `GREATER_THAN -> `String "GREATER_THAN"
  | `GREATER_EQUAL -> `String "GREATER_EQUAL"
;;

let binary_operation_name_of_yojson : Yojson.Safe.t -> binary_operation_name = function
  | `String "ADD" -> `ADD
  | `String "SUBTRACT" -> `SUBTRACT
  | `String "MULTIPLY" -> `MULTIPLY
  | `String "DIVIDE" -> `DIVIDE
  | `String "AND" -> `AND
  | `String "OR" -> `OR
  | `String "EQUAL" -> `EQUAL
  | `String "NOT_EQUAL" -> `NOT_EQUAL
  | `String "LESS_THAN" -> `LESS_THAN
  | `String "LESS_EQUAL" -> `LESS_EQUAL
  | `String "GREATER_THAN" -> `GREATER_THAN
  | `String "GREATER_EQUAL" -> `GREATER_EQUAL
  | j ->
    Yojson.json_error
    @@ Printf.sprintf "Invalid binary operator name «%s»" (Yojson.Safe.to_string j)
;;

type unary_operation_name =
  [ `NOT
  | `MINUS
  ]

let yojson_of_unary_operation_name : unary_operation_name -> Yojson.Safe.t = function
  | `NOT -> `String "NOT"
  | `MINUS -> `String "MINUS"
;;

let unary_operation_name_of_yojson : Yojson.Safe.t -> unary_operation_name = function
  | `String "NOT" -> `NOT
  | `String "MINUS" -> `MINUS
  | j ->
    Yojson.json_error
    @@ Printf.sprintf "Invalid unary operator «%s»" (Yojson.Safe.to_string j)
;;

type t =
  [ `ColumnReferenceExpression of column_reference_expression (* 1 *)
  | `Literal of literal (* 3 *)
  | `Function of function_ (* 1 *)
  | `ColumnarBinaryOperation of columnar_binary_operation (* 1 *)
  | `ColumnarUnaryOperation of columnar_unary_operation (* 1 *)
  ]

and function_ =
  { function_name : function_name [@key "functionName"]
  ; arguments : t list
  }

and columnar_binary_operation =
  { b_operator : binary_operation_name [@key "operator"]
  ; b_left_operand : t [@key "leftOperand"]
  ; b_right_operand : t [@key "rightOperand"]
  }

and columnar_unary_operation =
  { u_operator : unary_operation_name [@key "operator"]
  ; u_operand : t [@key "operand"]
  }
[@@deriving yojson]

let rec t_of_yojson : Yojson.Safe.t -> t =
  fun json ->
  WebUtils.Yj.alt
    [ ( "colref"
      , fun j -> `ColumnReferenceExpression (column_reference_expression_of_yojson j) )
    ; ("literal", fun j -> `Literal (literal_of_yojson j))
    ; ("function", fun j -> `Function (function__of_yojson j))
    ; ("binary", fun j -> `ColumnarBinaryOperation (columnar_binary_operation_of_yojson j))
    ; ("unary", fun j -> `ColumnarUnaryOperation (columnar_unary_operation_of_yojson j))
    ]
    json

and function__of_yojson : Yojson.Safe.t -> function_ = function
  | `Assoc props ->
    (match List.assoc_opt "functionName" props, List.assoc_opt "arguments" props with
     | Some fname_json, Some args_json ->
       { function_name = [%of_yojson: function_name] fname_json
       ; arguments = [%of_yojson: t list] args_json
       }
     | _ -> Yojson.json_error "function_")
  | _ -> Yojson.json_error "function_"

and columnar_binary_operation_of_yojson : Yojson.Safe.t -> columnar_binary_operation
  = function
  | `Assoc props ->
    (match
       ( List.assoc_opt "operator" props
       , List.assoc_opt "leftOperand" props
       , List.assoc_opt "rightOperand" props )
     with
     | Some op, Some l, Some r ->
       { b_operator = [%of_yojson: binary_operation_name] op
       ; b_left_operand = [%of_yojson: t] l
       ; b_right_operand = [%of_yojson: t] r
       }
     | o, l, r ->
       Yojson.json_error
         Yojson.Safe.(
           Printf.sprintf
             "Invalid props: %s, %s, %s"
             (o |> yojson_of_option Fun.id |> to_string)
             (l |> yojson_of_option Fun.id |> to_string)
             (r |> yojson_of_option Fun.id |> to_string)))
  | j -> Yojson.json_error (Printf.sprintf "Not assoc: %s" (Yojson.Safe.to_string j))

and columnar_unary_operation_of_yojson : Yojson.Safe.t -> columnar_unary_operation
  = function
  | `Assoc props ->
    (match List.assoc_opt "operator" props, List.assoc_opt "operand" props with
     | Some op, Some a ->
       { u_operator = [%of_yojson: unary_operation_name] op
       ; u_operand = [%of_yojson: t] a
       }
     | o, a ->
       Yojson.json_error
         Yojson.Safe.(
           Printf.sprintf
             "Invalid props: %s, %s"
             (o |> yojson_of_option Fun.id |> to_string)
             (a |> yojson_of_option Fun.id |> to_string)))
  | j -> Yojson.json_error (Printf.sprintf "Not assoc: %s" (Yojson.Safe.to_string j))
;;

let yojson_of_t : t -> Yojson.Safe.t = function
  | `ColumnReferenceExpression e -> yojson_of_column_reference_expression e
  | `Literal e -> yojson_of_literal e
  | `Function e -> yojson_of_function_ e
  | `ColumnarBinaryOperation e -> yojson_of_columnar_binary_operation e
  | `ColumnarUnaryOperation e -> yojson_of_columnar_unary_operation e
;;
