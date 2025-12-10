open Lib.Column
open Core

let parse_value (type_ : col) value =
  match type_ with
  | `ColInt ->
    (try `DataInt (Int64.of_string value) |> Result.ok with
     | Failure details ->
       Error
         QueryTask.
           { message = "Failed to parse INT64"
           ; details =
               Printf.sprintf "Failed to parse value «%s». Error: %s" value details
           })
  | `ColVarchar -> Result.Ok (`DataVarchar value)
;;

(** [parse_channel ~selector ~columns csv_channel] selects the fields from 
    records of [csv_channel] using [selector], and parses them into [columns]. 
    *)
let parse_channel ~selector ~columns csv_channel =
  let aux () =
    try
      Logger.log `Debug "parsing...";
      flush_all ();
      Csv.next csv_channel
      |> Array.of_list
      |> selector
      |> Array.combine columns
      |> Array.map (fun (t, v) ->
        parse_value t v |> Utils.Unwrap.result ~exc:QueryTask.make_error)
      |> Option.some
    with
    | Csv.Failure (row, field, msg) ->
      raise
      @@ QueryTask.make_error
           { message = "Invalid CSV file"
           ; details = Printf.sprintf "row: %d, field: %d, message: %s" row field msg
           }
    | End_of_file -> None
  in
  Logger.log `Debug "[%s] creating seq" __FUNCTION__;
  flush_all ();
  Seq.of_dispenser aux
;;

let read_csv ~has_header path =
  Logger.log `Debug "[%s] path=%s" __FUNCTION__ path;
  flush_all ();
  let cin =
    Unix.openfile path [ O_RDONLY ] 0o640
    |> Unix.in_channel_of_descr
    |> Csv.of_channel ~has_header
  in
  Logger.log `Debug "opened csv";
  flush_all ();
  cin
;;
