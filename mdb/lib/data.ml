module Types = struct
  type _ t = DataInt : int64 -> int64 t | DataVarchar : string -> string t

  let to_type_str : type a. a t -> string = function
    | DataInt _ -> "int64"
    | DataVarchar _ -> "varchar"

  type 'a getter = 'a t -> ('a, string) result

  let make_casting_error t d =
    Result.error
    @@ Printf.sprintf "Expected %s but got %s instead" t (to_type_str d)

  let get_int64 : int64 getter = function DataInt i -> Result.ok i
  let get_varchar : string getter = function DataVarchar s -> Result.ok s
end

type 'a data_record = (string * 'a Types.t) list
