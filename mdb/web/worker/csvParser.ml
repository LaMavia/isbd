open Lib.Column

let parse_value (type_ : col) value =
  let open Models.MultipleProblemsError in
  match type_ with
  | `ColInt ->
    (try `DataInt (Int64.of_string value) |> Result.ok with
     | Failure details ->
       Result.Error
         [ { error = "Failed to parse INT64"
           ; context =
               Some (Printf.sprintf "Failed to parse value «%s». Error: %s" value details)
           }
         ])
  | `ColVarchar -> Result.Ok (`DataVarchar value)
;;

(** [parse_channel ~selector ~columns csv_channel] selects the fields from 
    records of [csv_channel] using [selector], and parses them into [columns]. 
    *)
let parse_channel ~selector ~columns csv_channel =
  let open Models in
  let aux () =
    try
      let v =
        Csv.next csv_channel
        |> selector
        |> List.combine columns
        |> List.map (fun (t, v) ->
          parse_value t v |> Utils.Unwrap.result ~exc:MultipleProblemsError.make)
        |> Array.of_list
      in
      Array.iter
        (function
          | `DataInt d -> Printf.eprintf "DI %Ld " d
          | `DataVarchar s -> Printf.eprintf "DVC %s " s)
        v;
      Printf.eprintf "\n";
      Option.some v
    with
    | Csv.Failure (row, field, msg) ->
      raise
      @@ MultipleProblemsError.make
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
  |> Csv.of_channel ~has_header
;;
