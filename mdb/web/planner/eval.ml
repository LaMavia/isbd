open Models
open Lib
(* open Core *)

type t = (Data.Types.t, MultipleProblemsError.t) result

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

let eval_lit : ColumnExpression.literal -> Data.Types.t = function
  | `LitVarchar s -> `DataVarchar s
  | `LitInt i -> `DataInt i
  | `LitBool b -> `DataBool b
;;

let make_eval (td : Metastore.TableData.t) =
  let column_lookup = make_column_lookup td in
  fun (record : Data.data_record) ->
    Cse.with_cse (fun _eval e ->
      let open ColumnExpression in
      match e with
      | `Literal l -> Ok (eval_lit l)
      | `ColumnReferenceExpression { column_name; _ } ->
        Ok (Hashtbl.find column_lookup column_name |> Array.get record)
      | _ ->
        Error
          [ Exc.unimplemented
              ~context:
                (Some ([%yojson_of: ColumnExpression.t] e |> Yojson.Safe.to_string))
              ()
          ])
;;
