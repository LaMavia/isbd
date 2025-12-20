let mmap_result f arr =
  match List.map f arr with
  | [] -> Ok []
  | x :: xs ->
    x
    |> Result.map (fun x -> [ x ])
    |> List.fold_right
         (fun e u ->
            match e with
            | Ok r -> Result.map (List.cons r) u
            | Error excs ->
              Error (List.append excs (Result.fold ~ok:(Fun.const []) ~error:Fun.id u)))
         (List.rev xs)
    |> Result.map List.rev
;;
