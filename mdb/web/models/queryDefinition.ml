type t =
  | QD_SelectQuery of SelectQuery.t
  | QD_CopyQuery of CopyQuery.t

module Internal = struct
  let select_query_of_yojson json = QD_SelectQuery (SelectQuery.t_of_yojson json)
  let copy_query_of_yojson json = QD_CopyQuery (CopyQuery.t_of_yojson json)
end

let t_of_yojson json =
  WebUtils.Yj.alt Internal.[ select_query_of_yojson; copy_query_of_yojson ] json
;;

let yojson_of_t = function
  | QD_SelectQuery q -> SelectQuery.yojson_of_t q
  | QD_CopyQuery q -> CopyQuery.yojson_of_t q
;;
