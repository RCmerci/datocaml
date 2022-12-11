open Coll
module ResultMonad = Xresult.MonadMake (String)
open ResultMonad.O

type attr_value_B = Base64EncodedS of string

let attr_value_B_to_yojson (Base64EncodedS s) = s |> [%to_yojson: string]

let attr_value_B_of_yojson yojson =
  match yojson with
  | `String s -> Ok (Base64EncodedS s)
  | _ -> Error "attr_value_B_of_yojson"

type 'a assoc_list = (string * 'a) list [@@deriving yojson]

let assoc_list_to_yojson a_to_yojson l =
  `Assoc (List.map (fun (k, v) -> (k, a_to_yojson v)) l)

let assoc_list_of_yojson a_of_yojson yojson =
  match yojson with
  | `Assoc l ->
    List.fold_left
      (fun acc (k, v) ->
        match acc with
        | Error _ -> acc
        | Ok acc' -> (
          match a_of_yojson v with
          | Ok v' -> Ok ((k, v') :: acc')
          | Error s -> Error s))
      (Ok []) l
  | _ -> Error "assoc_list_of_yojson"

type string_attr_value_list = attr_value assoc_list

and attr_value =
  | B of attr_value_B
  | BOOL of bool
  | BS of attr_value_B list
  | L of attr_value list
  | M of string_attr_value_list
  | N of float
  | NS of float list
  | NULL of bool
  | S of string
  | SS of string list

let rec attr_value_to_yojson v : Yojson.Safe.t =
  match v with
  | B v -> `Assoc [ ("B", attr_value_B_to_yojson v) ]
  | BOOL v -> `Assoc [ ("BOOL", [%to_yojson: bool] v) ]
  | BS v -> `Assoc [ ("BS", `List (List.map attr_value_B_to_yojson v)) ]
  | L v -> `Assoc [ ("L", `List (List.map attr_value_to_yojson v)) ]
  | M v -> `Assoc [ ("M", string_attr_value_list_to_yojson v) ]
  | N v -> `Assoc [ ("N", [%to_yojson: float] v) ]
  | NS v -> `Assoc [ ("NS", `List (List.map [%to_yojson: float] v)) ]
  | NULL v -> `Assoc [ ("NULL", [%to_yojson: bool] v) ]
  | S v -> `Assoc [ ("S", [%to_yojson: string] v) ]
  | SS v -> `Assoc [ ("SS", `List (List.map [%to_yojson: string] v)) ]

and attr_value_of_yojson yojson =
  match yojson with
  | `Assoc [ ("B", v) ] ->
    let+ v' = attr_value_B_of_yojson v in
    B v'
  | `Assoc [ ("BOOL", v) ] ->
    let+ v' = [%of_yojson: bool] v in
    BOOL v'
  | `Assoc [ ("BS", `List v) ] ->
    let module X = Xlist.Fold_left_with_stop (struct
      type t = (attr_value_B list, string) result
    end) in
    let+ r =
      X.fold_left_with_stop
        (fun acc e ->
          match (attr_value_B_of_yojson e, acc) with
          | Ok v, Ok acc' -> Ok (v :: acc')
          | Error s, _ -> X.stop (Error s)
          | _ -> failwith "attr_value_of_yojson: unreachable bug")
        (Ok []) v
    in
    BS r
  | `Assoc [ ("L", `List v) ] ->
    let module X = Xlist.Fold_left_with_stop (struct
      type t = (attr_value list, string) result
    end) in
    let+ r =
      X.fold_left_with_stop
        (fun acc e ->
          match (attr_value_of_yojson e, acc) with
          | Ok v, Ok acc' -> Ok (v :: acc')
          | Error s, _ -> X.stop (Error s)
          | _ -> failwith "attr_value_of_yojson: unreachable bug")
        (Ok []) v
    in
    L r
  | `Assoc [ ("M", v) ] ->
    let+ r = string_attr_value_list_of_yojson v in
    M r
  | `Assoc [ ("N", v) ] -> (
    let* r = [%of_yojson: string] v in
    match float_of_string_opt r with
    | None -> Error "attr_value_of_yojson"
    | Some v -> Ok (N v))
  | `Assoc [ ("NS", `List v) ] ->
    let module X = Xlist.Fold_left_with_stop (struct
      type t = (float list, string) result
    end) in
    let+ r =
      X.fold_left_with_stop
        (fun acc e ->
          match ([%of_yojson: string] e, acc) with
          | Ok e', Ok acc' -> (
            match float_of_string_opt e' with
            | None -> X.stop (Error "attr_value_of_yojson")
            | Some v -> Ok (v :: acc'))
          | Error s, _ -> X.stop (Error s)
          | _ -> failwith "attr_value_of_yojson: unreachable bug")
        (Ok []) v
    in
    NS r
  | `Assoc [ ("NULL", v) ] ->
    let+ r = [%of_yojson: bool] v in
    NULL r
  | `Assoc [ ("S", v) ] ->
    let+ r = [%of_yojson: string] v in
    S r
  | `Assoc [ ("SS", `List v) ] ->
    let module X = Xlist.Fold_left_with_stop (struct
      type t = (string list, string) result
    end) in
    let+ r =
      X.fold_left_with_stop
        (fun acc e ->
          match ([%of_yojson: string] e, acc) with
          | Ok e', Ok acc' -> Ok (e' :: acc')
          | Error s, _ -> X.stop (Error s)
          | _ -> failwith "attr_value_of_yojson: unreachable bug")
        (Ok []) v
    in
    SS r
  | _ -> Error "attr_value_of_yojson"

and string_attr_value_list_of_yojson yojson =
  assoc_list_of_yojson attr_value_of_yojson yojson

and string_attr_value_list_to_yojson yojson =
  assoc_list_to_yojson attr_value_to_yojson yojson

type prim_and_range_key =
  { primary_key : string * attr_value
  ; range_key : (string * attr_value) option
  }
[@@deriving make]

let prim_and_range_key_to_yojson { primary_key; range_key } : Yojson.Safe.t =
  string_attr_value_list_to_yojson @@ (primary_key :: Option.to_list range_key)

let prim_and_range_key_of_yojson yojson =
  let* r = string_attr_value_list_of_yojson yojson in
  match r with
  | [ kv ] -> Ok { primary_key = kv; range_key = None }
  (* cannot figure out which one is range-key & primary-key *)
  | [ kv1; kv2 ] -> Ok { primary_key = kv1; range_key = Some kv2 }
  | _ -> Error "prim_and_range_key_of_yojson"

type item = string_attr_value_list [@@deriving yojson]

type expression_attribute_names = string assoc_list [@@deriving yojson]

type expression_attribute_values = string_attr_value_list [@@deriving yojson]

type consumed_capacity =
  { capacity_units : float option [@key "CapacityUnits"] [@default None]
  ; table_name : string option [@key "TableName"] [@default None]
  ; read_capacity_units : float option
        [@key "ReadCapacityUnits"] [@default None]
  ; write_capacity_units : float option
        [@key "WriteCapacityUnits"] [@default None]
  }
[@@deriving of_yojson { strict = false }]

type return_consumed_capacity =
  | INDEXES
  | TOTAL
  | NONE

let return_consumed_capacity_to_yojson = function
  | INDEXES -> `String "INDEXES"
  | TOTAL -> `String "TOTAL"
  | NONE -> `String "NONE"

type delete_request = { key : prim_and_range_key [@key "Key"] }
[@@deriving to_yojson]

type put_request = { item : item [@key "Item"] } [@@deriving to_yojson]

type write_request =
  | Put_request of put_request
  | Delete_request of delete_request

let write_request_to_yojson = function
  | Put_request v -> `Assoc [ ("PutRequest", [%to_yojson: put_request] v) ]
  | Delete_request v ->
    `Assoc [ ("DeleteRequest", [%to_yojson: delete_request] v) ]

type request_items = (string * write_request list) list

let request_items_to_yojson l =
  `Assoc
    (List.map
       (fun (tablename, wrl) ->
         (tablename, `List (List.map [%to_yojson: write_request] wrl)))
       l)

type batch_get_item_request_item =
  { consistent_read : bool option [@key "ConsistentRead"]
  ; expression_attribute_names : expression_attribute_names option
        [@key "ExpressionAttributeNames"]
  ; keys : prim_and_range_key list [@key "Keys"]
  }
[@@deriving yojson, make]

type batch_get_item_request_items = batch_get_item_request_item assoc_list
[@@deriving yojson]

type batch_get_item_response_responses = string_attr_value_list list assoc_list
[@@deriving yojson]

type return_values =
  | RETURN_VALUES_NONE
  | RETURN_VALUES_ALL_OLD
  | RETURN_VALUES_UPDATED_OLD
  | RETURN_VALUES_ALL_NEW
  | RETURN_VALUES_UPDATED_NEW

let return_values_to_yojson = function
  | RETURN_VALUES_NONE -> `String "NONE"
  | RETURN_VALUES_ALL_OLD -> `String "ALL_OLD"
  | RETURN_VALUES_UPDATED_OLD -> `String "UPDATED_OLD"
  | RETURN_VALUES_ALL_NEW -> `String "ALL_NEW"
  | RETURN_VALUES_UPDATED_NEW -> `String "UPDATED_NEW"

(* request & response *)

type list_tables_response = { table_names : string list [@key "TableNames"] }
[@@deriving of_yojson { strict = false }]

type get_item_request =
  { table_name : string [@key "TableName"]
  ; key : prim_and_range_key [@key "Key"]
  ; consistent_read : bool option [@key "ConsistentRead"]
  ; return_consumed_capacity : return_consumed_capacity option
        [@key "ReturnConsumedCapacity"]
  }
[@@deriving to_yojson, make]

type get_item_response =
  { item : item option [@key "Item"] [@default None]
  ; consumed_capacity : consumed_capacity option
        [@key "ConsumedCapacity"] [@default None]
  }
[@@deriving of_yojson { strict = false }]

type query_request =
  { table_name : string [@key "TableName"]
  ; key_condition_expression : string [@key "KeyConditionExpression"]
  ; expression_attribute_names : expression_attribute_names option
        [@key "ExpressionAttributeNames"]
  ; expression_attribute_values : expression_attribute_values option
        [@key "ExpressionAttributeValues"]
  ; filter_expression : string option [@key "FilterExpression"]
  ; limit : int option [@key "Limit"]
  ; consistent_read : bool option [@key "ConsistentRead"]
  ; index_name : string option [@key "IndexName"]
  ; exclusive_start_key : string_attr_value_list option
        [@key "ExclusiveStartKey"]
  ; return_consumed_capacity : return_consumed_capacity option
        [@key "ReturnConsumedCapacity"]
  }
[@@deriving to_yojson, make]

type query_response =
  { consumed_capacity : consumed_capacity option
        [@key "ConsumedCapacity"] [@default None]
  ; count : int [@key "Count"]
  ; items : item list [@key "Items"]
  ; last_evaluated_key : string_attr_value_list option
        [@key "LastEvaluatedKey"] [@default None]
  ; scanned_count : int [@key "ScannedCount"]
  }
[@@deriving of_yojson { strict = false }]

type batch_write_item_request_raw =
  { request_items : request_items [@key "RequestItems"]
  ; return_consumed_capacity : return_consumed_capacity option
        [@key "ReturnConsumedCapacity"]
  }
[@@deriving to_yojson, make]

type batch_write_item_request_request_items =
  | Put of item list
  | Delete of prim_and_range_key list

type batch_write_item_request =
  (string * batch_write_item_request_request_items) list

let batch_write_item_request_request_items_to_write_requests
    (v : batch_write_item_request_request_items) =
  match v with
  | Put items -> List.map (fun item -> Put_request { item }) items
  | Delete keys -> List.map (fun key -> Delete_request { key }) keys

let batch_write_item_request_to_raw (v : batch_write_item_request) =
  let r =
    List.map
      (fun (table, request_items) ->
        ( table
        , batch_write_item_request_request_items_to_write_requests request_items
        ))
      v
  in
  { request_items = r; return_consumed_capacity = Some TOTAL }

let partition_batch_write_item_request_raw ?(n = 25)
    { request_items; return_consumed_capacity } :
    batch_write_item_request_raw list =
  List.flatten
  @@ List.map
       (fun (table, l) ->
         List.map
           (fun l ->
             { request_items = [ (table, l) ]; return_consumed_capacity })
           (Xlist.partition_all_by_n l n))
       request_items

type batch_write_item_response =
  { consumed_capacity : consumed_capacity list option
        [@key "ConsumedCapacity"] [@default None]
  }
[@@deriving of_yojson { strict = false }]

type update_item_request =
  { table_name : string [@key "TableName"]
  ; key : prim_and_range_key [@key "Key"]
  ; update_expression : string option [@key "UpdateExpression"]
  ; condition_expression : string option [@key "ConditionExpression"]
  ; expression_attribute_names : expression_attribute_names option
        [@key "ExpressionAttributeNames"]
  ; expression_attribute_values : expression_attribute_values option
        [@key "ExpressionAttributeValues"]
  ; return_consumed_capacity : return_consumed_capacity option
        [@key "ReturnConsumedCapacity"]
  ; return_values : return_values option [@key "ReturnValues"]
  }
[@@deriving to_yojson, make]

type update_item_response =
  { attributes : string_attr_value_list option
        [@key "Attributes"] [@default None]
  ; consumed_capacity : consumed_capacity option
        [@key "ConsumedCapacity"] [@default None]
  }
[@@deriving of_yojson { strict = false }]

type batch_get_item_request =
  { request_items : batch_get_item_request_items [@key "RequestItems"]
  ; return_consumed_capacity : return_consumed_capacity option
        [@key "ReturnConsumedCapacity"]
  }
[@@deriving to_yojson, make]

type batch_get_item_response =
  { consumed_capacity : consumed_capacity list option
        [@key "ConsumedCapacity"] [@default None]
  ; responses : batch_get_item_response_responses [@key "Responses"]
  ; unprocessed_keys : batch_get_item_request_items [@key "UnprocessedKeys"]
  }
[@@deriving of_yojson { strict = false }]

type request =
  | List_tables
  | Get_item of get_item_request
  | Query of query_request
  | Batch_write_item of batch_write_item_request_raw
  | Update_item of update_item_request
  | Batch_get_item of batch_get_item_request

let request_to_yojson = function
  | List_tables -> `Assoc []
  | Get_item v -> [%to_yojson: get_item_request] v
  | Query v -> [%to_yojson: query_request] v
  | Batch_write_item v -> [%to_yojson: batch_write_item_request_raw] v
  | Update_item v -> [%to_yojson: update_item_request] v
  | Batch_get_item v -> [%to_yojson: batch_get_item_request] v
