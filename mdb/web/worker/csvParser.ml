open Lib.Column
open Core

let parse_value (type_ : col) value =
  match type_ with
  | `ColInt ->
    (try `DataInt (Int64.of_string value) |> Result.ok with
     | Failure details ->
       Error
         Models.MultipleProblemsError.
           [ { error = "Failed to parse INT64"
             ; context =
                 Some
                   (Printf.sprintf "Failed to parse value «%s». Error: %s" value details)
             }
           ])
  | `ColVarchar -> Result.Ok (`DataVarchar value)
;;

(** [parse_channel ~selector ~columns csv_channel] selects the fields from 
    records of [csv_channel] using [selector], and parses them into [columns]. 
    *)
let parse_channel ~selector ~columns csv_channel =
  let aux () =
    try
      let row = Csv.next csv_channel |> Array.of_list |> selector in
      if Array.length row <> Array.length columns
      then
        raise
          (Invalid_argument
             (Printf.sprintf
                "Expected row to be of length %d but got %d instead"
                (Array.length columns)
                (Array.length row)));
      row
      |> Array.combine columns
      |> Array.map (fun (t, v) ->
        parse_value t v |> Utils.Unwrap.result ~exc:QueryTask.make_error)
      |> Option.some
    with
    | Csv.Failure (row, field, msg) ->
      raise
      @@ QueryTask.make_error
           [ { error = "Invalid CSV file"
             ; context =
                 Some (Printf.sprintf "row: %d, field: %d, message: %s" row field msg)
             }
           ]
    | End_of_file -> None
  in
  Seq.of_dispenser aux
;;

let read_csv ~has_header path =
  Unix.openfile path [ O_RDONLY ] 0o640
  |> Unix.in_channel_of_descr
  |> Csv.of_channel ~has_header ~strip:true
;;
