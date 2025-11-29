type t =
  | Created [@name "CREATED"]
  | Planning [@name "PLANNING"]
  | Running [@name "RUNNING"]
  | Completed [@name "COMPLETED"]
  | Failed [@name "FAILED"]
[@@deriving yojson]
