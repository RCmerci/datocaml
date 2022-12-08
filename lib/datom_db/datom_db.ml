(*
schema type:
- :unique
- :ref-value

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
