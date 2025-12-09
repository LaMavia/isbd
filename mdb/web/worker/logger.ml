let prefix_of_level = function
  | `Error -> "\x1b[1;31mERROR"
  | `Info -> "\x1b[1;36mINFO"
;;

let log level =
  Format.ksprintf
    (Printf.eprintf
       "\x1b[1m[Worker:%s\x1b[0m\x1b[1m]\x1b[0m %s\n"
       (prefix_of_level level))
;;
