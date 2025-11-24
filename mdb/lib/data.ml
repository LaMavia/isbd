module Types = struct
  type t =
    | DataInt of int64
    | DataVarchar of string

  type data_stream = t Seq.t

  let to_type_str = function
    | DataInt _ -> "int64"
    | DataVarchar _ -> "varchar"
  ;;

  type 'a getter = t -> ('a, string) result

  let make_casting_error t d =
    Result.error @@ Printf.sprintf "Expected %s but got %s instead" t (to_type_str d)
  ;;

  let get_int64 : int64 getter = function
    | DataInt i -> Result.ok i
    | d -> make_casting_error "int64" d
  ;;

  let get_varchar : string getter = function
    | DataVarchar s -> Result.ok s
    | d -> make_casting_error "varchar" d
  ;;

  let to_str = function
    | DataInt i -> string_of_int (Int64.to_int i)
    | DataVarchar s -> Printf.sprintf "«%s»" s
  ;;
end

type data_record = Types.t array

let approx_size =
  let open Types in
  Seq.fold_left
    (Array.fold_left (fun u c ->
       u
       +
       match c with
       | DataInt _ -> 8
       | DataVarchar s -> String.length s))
    0
;;
