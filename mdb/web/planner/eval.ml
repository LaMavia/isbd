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

let eval_lit : ColumnExpression.literal -> Lib.Data.Types.t = function
  | `LitVarchar s -> `DataVarchar s
  | `LitInt i -> `DataInt i
  | `LitBool b -> `DataBool b
;;

let make_eval_ce (td : Metastore.TableData.t option) =
  let column_lookup = Option.map make_column_lookup td in
  fun (record : Lib.Data.data_record) ->
    Cse.with_cse (fun _eval e ->
      let open ColumnExpression in
      match e with
      | `Literal l -> Ok (eval_lit l)
      | `ColumnReferenceExpression { column_name; _ } ->
        Ok (Hashtbl.find (Option.get column_lookup) column_name |> Array.get record)
      | _ ->
        Error
          [ Exc.unimplemented
              ~context:
                (Some ([%yojson_of: ColumnExpression.t] e |> Yojson.Safe.to_string))
              ()
          ])
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
