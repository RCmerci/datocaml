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
partition-key: tenant#attr#value
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
- av -> table3 or table4

 *)
