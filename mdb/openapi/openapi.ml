module StringMap = Map.Make (String)

module type Serializable = sig
  type t [@@deriving yojson]
end

module type YoJsonMap = sig
  type v
  type t = v StringMap.t

  include Serializable with type t := t

  val of_list : (string * v) list -> t
  val add : string -> v -> t -> t
  val empty : t
end

module YoJsonMap (V : Serializable) = struct
  type v = V.t
  type t = V.t StringMap.t

  let of_yojson (m : Yojson.Safe.t) =
    let rec aux u = function
      | [] -> Result.Ok (List.rev u |> StringMap.of_list)
      | (k, vj) :: t -> (
          match V.of_yojson vj with
          | Result.Error e -> Result.Error e
          | Result.Ok v -> aux ((k, v) :: u) t)
    in
    match m with
    | `Assoc entries -> aux [] entries
    | _ ->
        Result.Error
          (Printf.sprintf "Expected object but received '%s'"
             (Yojson.Safe.to_string m))

  let to_yojson (v : t) : Yojson.Safe.t =
    `Assoc (StringMap.to_list v |> List.map (fun (k, v) -> (k, V.to_yojson v)))

  let of_list : (string * v) list -> t = StringMap.of_list
  let add : string -> v -> t -> t = StringMap.add
  let empty : t = StringMap.empty
end

module Reference = struct
  type t = { ref_ : string [@yojson.key "$ref"] } [@@deriving yojson]

  let make_of (type t) (to_name : t -> string) (top : [ `Components ])
      (typ : [ `Schemas | `Examples ]) (v : t) =
    let top_str = match top with `Components -> "components"
    and typ_str =
      match typ with `Schemas -> "schemas" | `Examples -> "examples"
    in
    { ref_ = Printf.sprintf "#/%s/%s/%s" top_str typ_str (to_name v) }
end

module ReferenceMap = YoJsonMap (Reference)

module rec Schema : sig
  type t [@@deriving yojson]

  val to_ref : string -> Reference.t
end = struct
  module SchemaObject = struct
    type t = {
      type_ : string; [@yojson.key "type"]
      required : string list;
      properties : SchemaMap.t;
    }
    [@@deriving yojson]
  end

  module SchemaString = struct
    type t = {
      type_ : [ `String [@yojson.name "string"] ]; [@yojson.key "type"]
      default : string option;
    }
    [@@deriving yojson]
  end

  module SchemaStringEnum = struct
    type t = {
      type_ : [ `String [@yojson.name "string"] ]; [@yojson.key "type"]
      default : string option;
      enum : string list;
    }
    [@@deriving yojson]
  end

  type t =
    | SchemaObject of SchemaObject.t
    | SchemaString of SchemaString.t
    | SchemaStringEnum of SchemaStringEnum.t

  let rec of_yojson (m : Yojson.Safe.t) : (t, string) result =
    let open Yojson.Safe.Util in
    try
      let type_ = m |> member "type" |> to_string_option in
      match type_ with
      | Some "object" ->
          SchemaObject
            {
              type_ = "object";
              required =
                [ m ] |> filter_member "required" |> flatten |> filter_string;
              properties = SchemaMap.empty;
            }
          |> Result.ok
    with Type_error (_, reason) -> Result.error reason

  (* let make_object ~required  *)

  let to_ref name = Reference.(make_of (Fun.const name) `Components `Schemas ())
end

and SchemaMap : sig
  include YoJsonMap with type v = Schema.t
end =
  YoJsonMap (Schema)

module MediaType = struct
  type t = { schema : Reference.t } [@@deriving yojson]
end

module MediaTypeMap = YoJsonMap (MediaType)

module Response = struct
  type t = { description : string; content : MediaTypeMap.t }
  [@@deriving yojson]
end

module ResponseMap = YoJsonMap (Response)

module Request = struct
  type t = {
    description : string;
    content : (string * MediaType.t) list;
    required : bool;
  }
  [@@deriving yojson]
end

module RequestMap = YoJsonMap (Request)

module Parameter = struct
  type t = {
    name : string;
    in_ :
      [ `Query [@yojson.name "query"]
      | `Header [@yojson.name "header"]
      | `Path [@yojson.name "path"]
      | `Cookie [@yojson.name "cookie"] ];
        [@yojson.key "in"]
    description : string;
    required : bool;
    deprecated : bool;
    schema : Reference.t;
  }
  [@@deriving yojson]

  let to_ref = Reference.make_of (fun v -> v.name)
end

module Operation = struct
  type t = {
    tags : string list;
    summary : string;
    description : string;
    operationId : string;
    parameters : Reference.t list;
    requestBody : Reference.t;
    responses : ReferenceMap.t;
  }
  [@@deriving yojson]

  and response_key = Default | Code of int

  let response_key_of_yojson (m : Yojson.Safe.t) :
      (response_key, string) Result.t =
    match m with
    | `String "default" -> Result.Ok Default
    | `Int code -> Result.Ok (Code code)
    | v ->
        Result.Error
          (Printf.sprintf "Invalid response key: %s" (Yojson.Safe.to_string v))

  let response_key_to_yojson = function
    | Default -> `String "default"
    | Code code -> `Int code
end

module Path = struct
  type t = {
    summary : string;
    description : string;
    get : Operation.t option;
    put : Operation.t option;
    post : Operation.t option;
    delete : Operation.t option;
    patch : Operation.t option;
    parameters : Reference.t list;
  }
  [@@deriving yojson]
end

module Info = struct
  type t = {
    title : string;
    summary : string;
    description : string;
    version : string;
  }
  [@@deriving yojson]
end

module Components = struct
  type t = {
    schemas : SchemaMap.t;
    responses : ResponseMap.t;
    requests : RequestMap.t;
  }
  [@@deriving yojson]
end

module Tag = struct
  type t = { name : string; description : string option } [@@deriving yojson]
end

module Server = struct
  type t = { url : string; description : string } [@@deriving yojson]
end

module PathJsonMap = YoJsonMap (Path)

type t = {
  openapi : string;
  info : Info.t;
  paths : PathJsonMap.t;
  components : Components.t;
  tags : Tag.t list;
}
[@@deriving yojson]

let empty ~title ~summary ~description ~version =
  {
    openapi = "3.1";
    info = Info.{ title; summary; description; version };
    tags = [];
    components =
      Components.
        {
          schemas = SchemaMap.of_list [];
          responses = ResponseMap.of_list [];
          requests = RequestMap.of_list [];
        };
    paths = StringMap.of_list [];
  }

let register_schema (name : string) (schema : Schema.t) (o : t) =
  {
    o with
    components =
      {
        o.components with
        schemas = SchemaMap.add name schema o.components.schemas;
      };
  }
