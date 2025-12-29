open Core
open Middleware

let app =
  let tq = TaskQueue.create () in
  let config = Config.of_env () in
  let metastore = Metastore.Store.load config in
  let worker_domain = Domain.spawn (Worker.Main.main metastore tq) in
  let uptime_clock = UptimeMiddleware.create () in
  let tried_stopping = ref false in
  Sys.(
    set_signal
      sigint
      (Signal_handle
         (function
           | _ when !tried_stopping ->
             Dream.log "[EXIT] Force-stopping";
             exit 1
           | _ ->
             tried_stopping := true;
             Dream.log "[EXIT] Stopping workers";
             TaskQueue.stop tq;
             Domain.join worker_domain;
             Dream.log "[EXIT] Clearing results";
             Metastore.Store.drop_all_results metastore;
             Dream.log "[EXIT] Saving metastore";
             Metastore.Store.save metastore;
             exit 0)));
  AppConfigMiddleware.middleware config
  @@ UptimeMiddleware.middleware uptime_clock
  @@ MetastoreMiddleware.middleware metastore
  @@ TaskQueueMiddleware.middleware tq
  @@ ErrorMiddleware.middleware
  @@ Routes.router
;;
