open Core
open Middleware

let app =
  let tq = TaskQueue.create () in
  let config = Config.of_env () in
  let metastore = Metastore.Store.load config in
  let worker_domain = Domain.spawn (Worker.Main.main metastore tq) in
  let uptime_clock = UptimeMiddleware.create () in
  Domain.at_exit (fun () ->
    Domain.join worker_domain;
    Metastore.Store.save config metastore);
  AppConfigMiddleware.middleware config
  @@ UptimeMiddleware.middleware uptime_clock
  @@ MetastoreMiddleware.middleware metastore
  @@ TaskQueueMiddleware.middleware tq
  (* @@ ErrorMiddleware.middleware *)
  @@ Routes.router
;;
