module Attr_schema = struct
  type t =
    | Unique
    | Reference
  [@@deriving ord]
end

type schema = (string * Attr_schema.t list) list

module SchemaMap = Map.Make (Attr_schema)
module AttrSet = Set.Make (String)

type rschema = AttrSet.t SchemaMap.t

type t =
  { schema : schema
  ; rschema : rschema
  }

let is_schema_type tp t attr =
  match SchemaMap.find_opt tp t.rschema with
  | Some attrs -> AttrSet.mem attr attrs
  | None -> false

let is_attr_unique = is_schema_type Attr_schema.Unique

let is_attr_reference = is_schema_type Attr_schema.Reference
