type t =
  | N of float
  | S of string
[@@deriving show, eq]

let to_attr_value = function
  | N n -> Dynamodb.Type.N n
  | S s -> Dynamodb.Type.S s
