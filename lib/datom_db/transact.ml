(* open Nanoid *)
open Dynamodb

let transact_add db_config
    (Datom.T (tenant, Datom.Id e, a, v) :
      (Datom.resolved, Datom.from_db_item_false) Datom.t) =
  let partition_key1 = Type.S (tenant ^ "#" ^ e) in
  let partition_key2 = Type.S (tenant ^ "#" ^ a) in
  let partition_key3 = Type.S (tenant ^ "#" ^ a) in
  let range_key1 = Type.S a in
  let range_key2 = Type.S e in
  let range_key3 = Datom.table3_range_key (Datom.Id e) v in
  let normal_key1 = Datom.to_attr_value v in
  let normal_key2 = normal_key1 in
  let request : Type.batch_write_item_request =
    [ ( Db_config.table1 db_config
      , Put
          [ [ (Db_config.table1_partition_key, partition_key1)
            ; (Db_config.table1_range_key, range_key1)
            ; (Db_config.table1_normal_key1, normal_key1)
            ]
          ] )
    ; ( Db_config.table2 db_config
      , Put
          [ [ (Db_config.table2_partition_key, partition_key2)
            ; (Db_config.table2_range_key, range_key2)
            ; (Db_config.table2_normal_key1, normal_key2)
            ]
          ] )
    ; ( Db_config.table3 db_config
      , Put
          [ [ (Db_config.table3_partition_key, partition_key3)
            ; (Db_config.table3_range_key, range_key3)
            ]
          ] )
    ]
  in
  request

let test_db_config : Db_config.t =
  { schema = []; rschema = Db_config.SchemaMap.empty; table_name = "datocaml" }

let test_tenant = "test_tenant"

let io_state env : Io.state = { consumed_capacity = 0.; env; config = None }

let f () =
  let req =
    transact_add test_db_config
      (Datom.T
         (test_tenant, Datom.Id (Nanoid.nanoid ()), "attr-1", Datom.S "attr-1-s"))
  in
  req

let q () =
  Eio_main.run @@ fun env ->
  let io =
    let open Io.Monad.O in
    let* a = Search.search test_db_config test_tenant (Search.A "attr-1") in
    let+ av =
      Search.search test_db_config test_tenant
        (Search.AV ("attr-1", Datom.S "attr-1-s"))
    in
    (a, av)
  in
  Io.run (io_state env) @@ io

let w req =
  let io = Io.batch_write_item req in
  Eio_main.run @@ fun env -> Io.run (io_state env) io
