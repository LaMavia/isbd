let zip_seqs (f : 'a array -> 'b) (seqs : 'a Seq.t array) =
  let aux () =
    let points = ref (Some []) in
    Array.iteri
      (fun i s ->
         Seq.uncons s
         |> Option.map (fun (h, t) ->
           seqs.(i) <- t;
           Printf.eprintf "h=%s\n" (Marshal.to_bytes h [] |> String.of_bytes);
           points := Option.map (fun ps -> h :: ps) !points)
         |> ignore)
      seqs;
    Printf.eprintf "seqs_len=%d\n" (Array.length seqs);
    match !points with
    | Option.None -> Option.None
    | Option.Some ps -> List.rev ps |> Array.of_list |> f |> Option.some
  in
  Seq.of_dispenser aux
;;
