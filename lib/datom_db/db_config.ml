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
  ; table_name : string
  }

let table1 t = t.table_name ^ "_1"

let table1_partition_key = "tenant+e"

let table1_range_key = "a"

let table1_normal_key1 = "v"

let table2 t = t.table_name ^ "_2"

let table2_partition_key = "tenant+a"

let table2_range_key = "e"

let table2_normal_key1 = "v"

let table3 t = t.table_name ^ "_3"

let table3_partition_key = "tenant+a"

let table3_range_key = "e+v"

let is_schema_type tp t attr =
  match SchemaMap.find_opt tp t.rschema with
  | Some attrs -> AttrSet.mem attr attrs
  | None -> false

let is_attr_unique = is_schema_type Attr_schema.Unique

let is_attr_reference = is_schema_type Attr_schema.Reference
