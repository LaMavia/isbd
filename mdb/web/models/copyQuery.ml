open Ppx_yojson_conv_lib.Yojson_conv.Primitives

type t =
  { source_filepath : string [@key "sourceFilepath"]
  ; destination_table_name : string [@key "destinationTableName"]
  ; destination_columns : string array option [@yojson.option] [@key "destinationColumns"]
  ; does_csv_contain_header : bool [@key "doesCsvContainHeader"] [@default false]
  }
[@@deriving yojson]
