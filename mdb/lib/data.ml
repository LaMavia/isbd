module Types = struct
  type t =
    [ `DataInt of int64
    | `DataVarchar of string
    | `DataBool of bool
    ]

  type data_stream = t Seq.t

  let to_type_str = function
    | `DataInt _ -> "int64"
    | `DataVarchar _ -> "varchar"
    | `DataBool _ -> "bool"
  ;;

  type 'a getter = t -> ('a, string) result

  let make_casting_error t d =
    Error (Printf.sprintf "Expected %s but got %s instead" t (to_type_str d))
  ;;

  let get_int64 : int64 getter = function
    | `DataInt i -> Ok i
    | d -> make_casting_error "int64" d
  ;;

  let get_varchar : string getter = function
    | `DataVarchar s -> Ok s
    | d -> make_casting_error "varchar" d
  ;;

  let get_bool : bool getter = function
    | `DataBool b -> Ok b
    | d -> make_casting_error "bool" d
  ;;

  let to_str = function
    | `DataInt i -> string_of_int (Int64.to_int i)
    | `DataVarchar s -> Printf.sprintf "«%s»" s
    | `DataBool b -> Printf.sprintf "%b" b
  ;;
end

type data_record = Types.t array

let approx_record_size : data_record -> int =
  Array.fold_left
    (fun u c ->
       u
       +
       match c with
       | `DataInt _ -> 8
       | `DataVarchar s -> String.length s
       | `DataBool _ -> 1)
    0
;;

let approx_size (records : data_record Seq.t) =
  Seq.fold_left (fun u r -> u + approx_record_size r) 0 records
;;

let string_of_record (r : data_record) =
  Array.to_list r |> List.map Types.to_str |> String.concat "; "
;;
