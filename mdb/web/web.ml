let app =
  let tq = Core.TaskQueue.create () in
  let config = Core.Config.of_env () in
  Middleware.AppConfigMiddleware.middleware config
  @@ Middleware.TaskQueueMiddleware.middleware tq
  @@ Routes.router
;;
