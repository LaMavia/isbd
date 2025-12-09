type t = { get_uptime : unit -> int64 }

let create () =
  let start = Unix.gettimeofday () |> Int64.of_float in
  { get_uptime = (fun () -> Int64.(sub (Unix.gettimeofday () |> of_float) start)) }
;;

let field : int64 Dream.field = Dream.new_field ~name:"uptime" ()

let middleware (u : t) (handler : Dream.handler) req =
  Dream.set_field req field (u.get_uptime ());
  handler req
;;
