open Core
open Middleware

let app =
  let tq = TaskQueue.create () in
  let config = Config.of_env () in
  AppConfigMiddleware.middleware config
  @@ TaskQueueMiddleware.middleware tq
  @@ Routes.router
;;
