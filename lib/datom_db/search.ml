(*
- table1
partition-key: tenant#eid
range-key: attr
normal-key: value
- table2
partition-key: tenant#attr
range-key: eid
normal-key: value
- table3
partition-key: tenant#attr
range-key: N#num-value#eid, S#STR-LENGTH#str-value#eid

search
- e -> table1
- ea -> table1
- eav -> table1
- a -> table2
- av -> table3

 *)
open Coll
open Dynamodb
open Io.Monad.O
module ResultO = Xresult.MonadMake (String)

type pattern =
  | E : string -> pattern
  | EA : string * Datom.attr -> pattern
  | EAV : string * Datom.attr * (_, _) Datom.value -> pattern
  | A : Datom.attr -> pattern
  | AV : Datom.attr * (_, _) Datom.value -> pattern

let rec search (db_config : Db_config.t) tenant (pattern : pattern) :
    (Datom.resolved, Datom.from_db_item_true) Datom.t list ResultO.t Io.t =
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
    List.map (Datom.from_table1_item db_config) r.items
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
    [ Datom.from_table1_item db_config r.item ]
  | EAV (e, a, v) -> (
    let+ r = search db_config tenant (EA (e, a)) in
    let open ResultO.O in
    let+ r in
    match r with
    | [ Datom.T_from_db_item (_, _, _, v') ] -> (
      match (v', v) with
      | Datom.N n', Datom.N n -> if n' = n then r else []
      | Datom.S s', Datom.S s -> if s' = s then r else []
      | Datom.Ref_value (Datom.Id e'), Datom.Ref_value (Datom.Id e) ->
        if e' = e then r else []
      | _ -> [])
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
    List.map (Datom.from_table2_item db_config) r.items
  | AV (a, v) ->
    let table_name = Db_config.table3 db_config in
    let partition_key = Type.S (tenant ^ "#" ^ a) in
    let partition_key_name = Db_config.table3_partition_key in
    let range_key_name = Db_config.table3_range_key in
    let range_key =
      Type.S
        (match v with
        | Datom.N n -> Datom.table3_range_key_num_for_query n
        | Datom.S s -> Datom.table3_range_key_str_for_query s
        | Datom.Ref_value (Datom.Id e) -> Datom.table3_range_key_str_for_query e)
    in
    let q =
      Type.make_query_request ~table_name
        ~key_condition_expression:"#k1=:v1 and begins_with(#k2,:v2)"
        ~expression_attribute_names:
          [ ("#k1", partition_key_name); ("#k2", range_key_name) ]
        ~expression_attribute_values:
          [ (":v1", partition_key); (":v2", range_key) ]
        ~return_consumed_capacity:Type.TOTAL ()
    in
    let+ r = Io.query q in
    Eio.traceln ~__POS__ "query: %a" Yojson.Safe.pp
      (Type.query_request_to_yojson q);
    let open ResultO.O in
    let+ r in
    List.map (Datom.from_table3_item db_config) r.items

let ensure_eid (type a) (eid : a Datom.eid) tenant (db_config : Db_config.t) :
    (string, string) result Io.t =
  match eid with
  | Datom.Id s -> Io.Monad.return (Ok s)
  | Datom.Ref (k, v) -> (
    if not (Db_config.is_attr_unique db_config k) then
      Io.Monad.return
      @@ Error
           (Printf.sprintf "Lookup ref attribute should be marked as Unique: %s"
              (Datom.show_eid eid))
    else
      let+ r = search db_config tenant (AV (k, v)) in
      let open ResultO.O in
      let* r in
      match r with
      | [ Datom.T_from_db_item (_, Datom.Id e, _, _) ] -> Ok e
      | _ -> Error "ensure_eid")
