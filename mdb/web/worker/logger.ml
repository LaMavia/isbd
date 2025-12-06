let prefix_of_level = function
  | `Error -> "[WORKER:ERROR]"
  | `Info -> "[WORKER:INFO]"
;;

let log level = Format.ksprintf (Printf.eprintf "%s %s\n" (prefix_of_level level))
