(*
- table1
partition-key: tenant#eid
range-key: attr
normal-key: value
- table2
partition-key: tenant#attr
range-key: eid
normal-key: value
- table3 (when attr-value is not :unique)
partition-key: tenant#attr#VALUE-TYPE#value
range-key: eid
- table4 (when attr-value is :unique)
partition-key: tenant#attr
range-key: value
normal-key: eid

search
- e -> table1
- ea -> table1
- eav -> table1
- a -> table2
- av ->
   1. when attr is :unique -> table4
   2. when value is small -> table3
   3. when value is huge -> table2

 *)
open Coll
open Dynamodb
open Io.Monad.O

module ResultO = Xresult.MonadMake (struct
  type t = Yojson.Safe.t
end)

type pattern =
  | E of string
  | EA of (string * Attr.t)
  | EAV of (string * Attr.t * Value.t)
  | A of Attr.t
  | AV of (Attr.t * Value.t)

let rec search (db_config : Db_config.t) tenant pattern :
    Datom.t list ResultO.t Io.t =
  match pattern with
  | E e ->
    let table_name = Db_config.table1 db_config in
    let partition_key = Type.S (tenant ^ "#" ^ e) in
    let+ r =
      Io.query
        (Type.make_query_request ~table_name ~key_condition_expression:"#k=:v"
           ~expression_attribute_names:
             [ ("#k", Db_config.table1_partition_key) ]
           ~expression_attribute_values:[ (":v", partition_key) ]
           ~return_consumed_capacity:Type.TOTAL ())
    in
    let open ResultO.O in
    let+ r in
    List.map Datom.from_table1_item r.items
  | EA (e, a) ->
    let table_name = Db_config.table1 db_config in
    let partition_key = Type.S (tenant ^ "#" ^ e) in
    let range_key = Type.S a in
    let+ r =
      Io.get_item
        (Type.make_get_item_request ~table_name
           ~key:
             (Type.make_prim_and_range_key
                ~primary_key:(Db_config.table1_partition_key, partition_key)
                ~range_key:(Db_config.table1_range_key, range_key)
                ())
           ~return_consumed_capacity:Type.TOTAL ())
    in
    let open ResultO.O in
    let+ r in
    [ Datom.from_table1_item r.item ]
  | EAV (e, a, v) -> (
    let+ r = search db_config tenant (EA (e, a)) in
    let open ResultO.O in
    let+ r in
    match r with
    | [ (_, _, _, v') ] -> if Value.equal v' v then r else []
    | _ -> failwith "unreachable")
  | A a ->
    let table_name = Db_config.table2 db_config in
    let partition_key = Type.S (tenant ^ "#" ^ a) in
    let+ r =
      Io.query
        (Type.make_query_request ~table_name ~key_condition_expression:"#k=:v"
           ~expression_attribute_names:
             [ ("#k", Db_config.table2_partition_key) ]
           ~expression_attribute_values:[ (":v", partition_key) ]
           ~return_consumed_capacity:Type.TOTAL ())
    in
    let open ResultO.O in
    let+ r in
    List.map Datom.from_table2_item r.items
  | AV (a, v) ->
    let is_unique = Db_config.is_attr_unique db_config a in
    let table_name, partition_key, partition_key_name =
      if is_unique then
        ( Db_config.table4 db_config
        , Type.S (tenant ^ "#" ^ a)
        , Db_config.table4_partition_key )
      else
        let v_str =
          match v with
          | Value.N n -> "N#" ^ string_of_float n
          | Value.S s -> "S#" ^ s
        in
        ( Db_config.table3 db_config
        , Type.S (tenant ^ "#" ^ a ^ "#" ^ v_str)
        , Db_config.table3_partition_key )
    in
    if is_unique then
      let+ r =
        Io.get_item
        @@ Type.make_get_item_request ~table_name
             ~key:
               (Type.make_prim_and_range_key
                  ~primary_key:(partition_key_name, partition_key)
                  ~range_key:(Db_config.table4_range_key, Value.to_attr_value v)
                  ())
             ~return_consumed_capacity:Type.TOTAL ()
      in
      let open ResultO.O in
      let+ r in
      [ Datom.from_table4_item r.item ]
    else
      let+ r =
        Io.query
        @@ Type.make_query_request ~table_name ~key_condition_expression:"#k=:v"
             ~expression_attribute_names:[ ("#k", partition_key_name) ]
             ~expression_attribute_values:[ (":v", partition_key) ]
             ~return_consumed_capacity:Type.TOTAL ()
      in
      let open ResultO.O in
      let+ r in
      List.map Datom.from_table3_item r.items
