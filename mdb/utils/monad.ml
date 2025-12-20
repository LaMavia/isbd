let mmap_result f arr =
  List.fold_right
    (fun e u ->
       match e with
       | Ok r -> Result.map (List.cons r) u
       | Error excs ->
         Error (List.append excs (Result.fold ~ok:(Fun.const []) ~error:Fun.id u)))
    (List.map f arr)
    (Ok [])
;;
