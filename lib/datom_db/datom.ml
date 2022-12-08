open Dynamodb

type attr = string [@@deriving show, eq]

type resolved = private Resolved_and_this_is_not_used

type unresolved = private Unresolved_and_this_is_not_used

type num_v = private Num_v

type string_v = private String_v

type _ eid =
  | Id : string -> resolved eid
  | Ref : (attr * (string_v, resolved) value) -> unresolved eid

and (_, _) value =
  | N : int -> (num_v, resolved) value
  | S : string -> (string_v, resolved) value
  | Ref_value : resolved eid -> (_, unresolved) value

let show_eid (type a) (eid : a eid) =
  match eid with
  | Id s -> Format.sprintf "Id %s" s
  | Ref (a, _v) -> Printf.sprintf "Ref %s, <value>" a

let show_value (type a b) (v : (a, b) value) =
  match v with
  | N n -> Format.sprintf "N %i" n
  | S s -> Format.sprintf "S %s" s
  | Ref_value e -> Format.sprintf "Ref_value %s" (show_eid e)

let equal_eid (e1 : resolved eid) e2 =
  match (e1, e2) with
  | Id e1, Id e2 -> e1 = e2

let equal_value (type a) (v1 : (a, resolved) value) (v2 : (a, resolved) value) =
  match (v1, v2) with
  | N v1, N v2 -> v1 = v2
  | S v1, S v2 -> v1 = v2

let to_attr_value (type a b) (v : (a, b) value) =
  match v with
  | N n -> Dynamodb.Type.N (float_of_int n)
  | S s -> Dynamodb.Type.S s
  | Ref_value (Id e) -> Dynamodb.Type.S e

type from_db_item_true = private From_db_item_true

type from_db_item_false = private From_db_item_false

(* tenant, e, a, v *)
type ('resolved_e, 'from_db_item) t =
  | T : string * 'e eid * attr * (_, _) value -> ('e, from_db_item_false) t
  | T_from_db_item :
      string * resolved eid * attr * (_, _) value
      -> (resolved, from_db_item_true) t

type temp_v =
  | Temp_s of string
  | Temp_n of int

type t' =
  { tenant : string option
  ; e : string option
  ; a : attr option
  ; v : temp_v option
  }

let empty_t' = { tenant = None; e = None; a = None; v = None }

let re_split_pattern = Str.regexp "#"

let from_table1_item db_config (item : Type.item) : (_, from_db_item_true) t =
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
          { acc with v = Some (Temp_s v') }
        | k, Type.N v' when k = Db_config.table1_normal_key1 ->
          { acc with v = Some (Temp_n (int_of_float v')) }
        | _ -> failwith @@ Printf.sprintf "bad table1 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (
    match v with
    | Temp_s s ->
      if Db_config.is_attr_reference db_config a then
        T_from_db_item (tenant, Id e, a, Ref_value (Id s))
      else T_from_db_item (tenant, Id e, a, S s)
    | Temp_n n -> T_from_db_item (tenant, Id e, a, N n))
  | _ -> failwith "bad table1 item"

let from_table2_item db_config (item : Type.item) : (_, from_db_item_true) t =
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
          { acc with v = Some (Temp_s v') }
        | k, Type.N v' when k = Db_config.table2_normal_key1 ->
          { acc with v = Some (Temp_n (int_of_float v')) }
        | _ -> failwith @@ Printf.sprintf "bad table2 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (
    match v with
    | Temp_s s ->
      if Db_config.is_attr_reference db_config a then
        T_from_db_item (tenant, Id e, a, Ref_value (Id s))
      else T_from_db_item (tenant, Id e, a, S s)
    | Temp_n n -> T_from_db_item (tenant, Id e, a, N n))
  | _ -> failwith "bad table2 item"

let from_table3_item db_config (item : Type.item) : (_, from_db_item_true) t =
  let t' =
    List.fold_left
      (fun acc (k, v) ->
        match (k, v) with
        | k, Type.S k' when k = Db_config.table3_partition_key -> (
          (* partition-key: tenant#attr *)
          match String.split_on_char '#' k' with
          | [ tenant; a ] -> { acc with tenant = Some tenant; a = Some a }
          | _ -> failwith @@ Printf.sprintf "bad table3_partition_key: %s" k')
        | k, Type.S v' when k = Db_config.table3_range_key -> (
          match Str.bounded_split re_split_pattern v' 3 with
          | [ "N"; num_v; eid ] ->
            { acc with e = Some eid; v = Some (Temp_n (int_of_string num_v)) }
          | [ "S"; str_len; tail ] ->
            let str_len = int_of_string str_len in
            let str_v = String.sub tail 0 str_len in
            let eid = String.sub tail str_len (String.length tail - str_len) in
            { acc with e = Some eid; v = Some (Temp_s str_v) }
          | _ -> failwith @@ Printf.sprintf "bad table3_range_key: %s" v')
        | _ -> failwith @@ Printf.sprintf "bad table3 item: %s" k)
      empty_t' item
  in
  let { tenant; e; a; v } = t' in
  match (tenant, e, a, v) with
  | Some tenant, Some e, Some a, Some v -> (
    match v with
    | Temp_s s ->
      if Db_config.is_attr_reference db_config a then
        T_from_db_item (tenant, Id e, a, Ref_value (Id s))
      else T_from_db_item (tenant, Id e, a, S s)
    | Temp_n n -> T_from_db_item (tenant, Id e, a, N n))
  | _ -> failwith "bad table3 item"

let padding_0 s = String.make (20 - String.length s) '0' ^ s

let rec table3_range_key :
    type a b. resolved eid -> (a, b) value -> Type.attr_value =
 fun (Id eid) v ->
  match v with
  | N n -> Type.S ("N#" ^ padding_0 (string_of_int n) ^ "#" ^ eid)
  | S s ->
    Type.S ("S#" ^ (string_of_int @@ String.length s) ^ "#" ^ s ^ "#" ^ eid)
  | Ref_value (Id r) -> table3_range_key (Id eid) (S r)

let table3_range_key_num_range_for_query min max =
  let min' = "N#" ^ padding_0 (string_of_int min) ^ "#" in
  let max' = "N#" ^ padding_0 (string_of_int max) ^ "#~" in
  (min', max')

let table3_range_key_num_for_query n = "N#" ^ padding_0 (string_of_int n) ^ "#"

let table3_range_key_str_for_query s =
  "S#" ^ (string_of_int @@ String.length s) ^ "#" ^ s ^ "#"
