type t =
  | QD_SelectQuery of SelectQuery.t
  | QD_CopyQuery of CopyQuery.t
  | QD_SelectAllQuery of SelectAllQuery.t

module Internal = struct
  let select_query_of_yojson json = QD_SelectQuery (SelectQuery.t_of_yojson json)
  let copy_query_of_yojson json = QD_CopyQuery (CopyQuery.t_of_yojson json)

  let select_all_query_of_yojson json =
    QD_SelectAllQuery (SelectAllQuery.t_of_yojson json)
  ;;
end

let t_of_yojson json =
  WebUtils.Yj.alt
    Internal.
      [ "select", select_query_of_yojson
      ; "copy", copy_query_of_yojson
      ; "select_all", select_all_query_of_yojson
      ]
    json
;;

let yojson_of_t = function
  | QD_SelectQuery q -> SelectQuery.yojson_of_t q
  | QD_CopyQuery q -> CopyQuery.yojson_of_t q
  | QD_SelectAllQuery q -> SelectAllQuery.yojson_of_t q
;;
