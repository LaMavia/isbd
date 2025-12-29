let buffer_size =
  Sys.getenv_opt "BUFFER_SIZE" |> Option.fold ~none:32_000_000 ~some:int_of_string
;;
