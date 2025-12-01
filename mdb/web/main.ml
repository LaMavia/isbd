open Core
open Middleware

let app =
  let tq = TaskQueue.create () in
  let config = Config.of_env () in
  let metastore = Metastore.Store.load config in
  AppConfigMiddleware.middleware config
  @@ MetastoreMiddleware.middleware metastore
  @@ TaskQueueMiddleware.middleware tq
  (* @@ ErrorMiddleware.middleware *)
  @@ Routes.router
;;
