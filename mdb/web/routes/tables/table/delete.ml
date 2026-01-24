open Middleware
open Core
open Models

let handler (req : Dream.request) =
  let ms = Dream.field req MetastoreMiddleware.field |> Option.get
  and tq = Dream.field req TaskQueueMiddleware.field |> Option.get in
  let ticket = TaskQueue.get_ticket tq in
  let table_id = Dream.param req "table_id" |> Core.Uuid.of_string in
  TaskQueue.with_ticket tq ticket
  @@ fun () ->
  match Metastore.Store.lookup_table_by_id table_id ms |> Metastore.TableData.inc_ref with
  | Some td ->
    Metastore.Store.with_table (Some td) ms (fun () ->
      Atomic.set td.sheduled_for_deletion true);
    Dream.empty `OK
  | None ->
    MultipleProblemsError.
      { problems =
          [ { error = "Table not found"
            ; context =
                Option.some
                @@ Printf.sprintf
                     "Table with id=%s doesn't exist"
                     (Core.Uuid.to_string table_id)
            }
          ]
      }
    |> [%yojson_of: MultipleProblemsError.t]
    |> Yojson.Safe.to_string
    |> Dream.json ~status:`Not_Found
;;
