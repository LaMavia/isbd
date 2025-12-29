let merge_workers =
  Sys.getenv_opt "MERGE_WORKERS" |> Option.fold ~none:2 ~some:int_of_string
;;

let max_k_merge = Sys.getenv_opt "MAX_K_MERGE" |> Option.fold ~none:10 ~some:int_of_string
