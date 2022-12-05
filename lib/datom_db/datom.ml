open Dynamodb

(* tenant, e, a, v *)
type t = string * string * Attr.t * Value.t

type t' =
  { tenant : string option
  ; e : string option
  ; a : Attr.t option
  ; v : Value.t option
  }
[@@deriving show]

let empty_t' = { tenant = None; e = None; a = None; v = None }

let re_split_pattern = Str.regexp "#"

let from_table1_item (item : Type.item) : t =
  let t' =
    List.fold_left
      (fun acc (k, v) ->
        match (k, v) with
        | k, Type.S v' when k = Db_config.table1_partition_key -> (
          match String.split_on_char '#' v' with
          | [ tenant; e ] -> { acc with tenant = Some tenant; e = Some e }
          | _ -> failwith @@ Printf.sprintf "bad table1_partition_key: %s" v')
        | k, Type.S v' when k = Db_config.table1_range_key ->
          { acc with a = Some v' }
        | k, Type.S v' when k = Db_config.table1_normal_key1 ->
          { acc with v = Some (Value.S v') }
        | k, Type.N v' when k = Db_config.table1_normal_key1 ->
          { acc with v = Some (Value.N v') }
        | _ -> failwith @@ Printf.sprintf "bad table1 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (tenant, e, a, v)
  | _ -> failwith @@ Printf.sprintf "bad table1 item: %s" (show_t' t')

let from_table2_item (item : Type.item) : t =
  let t' =
    List.fold_left
      (fun acc (k, v) ->
        match (k, v) with
        | k, Type.S v' when k = Db_config.table2_partition_key -> (
          match String.split_on_char '#' v' with
          | [ tenant; a ] -> { acc with tenant = Some tenant; a = Some a }
          | _ -> failwith @@ Printf.sprintf "bad table2_partition_key: %s" v')
        | k, Type.S v' when k = Db_config.table2_range_key ->
          { acc with e = Some v' }
        | k, Type.S v' when k = Db_config.table2_normal_key1 ->
          { acc with v = Some (Value.S v') }
        | k, Type.N v' when k = Db_config.table2_normal_key1 ->
          { acc with v = Some (Value.N v') }
        | _ -> failwith @@ Printf.sprintf "bad table2 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (tenant, e, a, v)
  | _ -> failwith @@ Printf.sprintf "bad table2 item: %s" (show_t' t')

let from_table3_item (item : Type.item) : t =
  let t' =
    List.fold_left
      (fun acc (k, v) ->
        match (k, v) with
        | k, Type.S v' when k = Db_config.table3_partition_key -> (
          (* partition-key: tenant#attr#VALUE-TYPE#value *)
          match Str.bounded_split re_split_pattern v' 4 with
          | [ tenant; a; vtype; v ] -> (
            match vtype with
            | "N" ->
              { acc with
                tenant = Some tenant
              ; a = Some a
              ; v = Some (Value.N (float_of_string v))
              }
            | "S" ->
              { acc with
                tenant = Some tenant
              ; a = Some a
              ; v = Some (Value.S v)
              }
            | _ -> failwith "unreachable")
          | _ -> failwith @@ Printf.sprintf "bad table3_partition_key: %s" v')
        | k, Type.S v' when k = Db_config.table3_range_key ->
          { acc with e = Some v' }
        | _ -> failwith @@ Printf.sprintf "bad table3 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (tenant, e, a, v)
  | _ -> failwith @@ Printf.sprintf "bad table3 item: %s" (show_t' t')

let from_table4_item (item : Type.item) : t =
  let t' =
    List.fold_left
      (fun acc (k, v) ->
        match (k, v) with
        | k, Type.S v' when k = Db_config.table4_partition_key -> (
          match String.split_on_char '#' v' with
          | [ tenant; a ] -> { acc with tenant = Some tenant; a = Some a }
          | _ -> failwith @@ Printf.sprintf "bad table4_partition_key: %s" v')
        | k, Type.S v' when k = Db_config.table4_range_key ->
          { acc with v = Some (Value.S v') }
        | k, Type.N v' when k = Db_config.table4_range_key ->
          { acc with v = Some (Value.N v') }
        | k, Type.S v' when k = Db_config.table4_normal_key1 ->
          { acc with e = Some v' }
        | _ -> failwith @@ Printf.sprintf "bad table4 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (tenant, e, a, v)
  | _ -> failwith @@ Printf.sprintf "bad table4 item: %s" (show_t' t')
