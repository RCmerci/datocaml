module Credential : sig
  val get_credential :
       ?which:string
    -> < fs : #Eio__.Fs.dir Eio.Path.t ; .. >
    -> (string * string) option

  val get_region :
    ?which:string -> < fs : #Eio__.Fs.dir Eio.Path.t ; .. > -> string option
end

module Type : sig
  type attr_value_B = Base64EncodedS of string

  type string_attr_value_list = (string * attr_value) list

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

  type prim_and_range_key =
    { primary_key : string * attr_value
    ; range_key : (string * attr_value) option
    }

  val make_prim_and_range_key :
       primary_key:string * attr_value
    -> ?range_key:string * attr_value
    -> unit
    -> prim_and_range_key

  type item = string_attr_value_list

  type expression_attribute_names = (string * string) list

  type expression_attribute_values = string_attr_value_list

  type consumed_capacity =
    { capacity_units : float option
    ; table_name : string option
    ; read_capacity_units : float option
    ; write_capacity_units : float option
    }

  type return_consumed_capacity =
    | INDEXES
    | TOTAL
    | NONE

  type delete_request = { key : prim_and_range_key }

  type put_request = { item : item }

  type write_request =
    | Put_request of put_request
    | Delete_request of delete_request

  type request_items = (string * write_request list) list

  type batch_get_item_request_item =
    { consistent_read : bool option
    ; expression_attribute_names : expression_attribute_names option
    ; keys : prim_and_range_key list
    }

  type batch_get_item_request_items =
    (string * batch_get_item_request_item) list

  type batch_get_item_response_responses =
    (string * string_attr_value_list list) list

  type return_values =
    | RETURN_VALUES_NONE
    | RETURN_VALUES_ALL_OLD
    | RETURN_VALUES_UPDATED_OLD
    | RETURN_VALUES_ALL_NEW
    | RETURN_VALUES_UPDATED_NEW

  type list_tables_response = { table_names : string list }

  type get_item_request =
    { table_name : string
    ; key : prim_and_range_key
    ; consistent_read : bool option
    ; return_consumed_capacity : return_consumed_capacity option
    }

  val get_item_request_to_yojson : get_item_request -> Yojson.Safe.t

  val make_get_item_request :
       table_name:string
    -> key:prim_and_range_key
    -> ?consistent_read:bool
    -> ?return_consumed_capacity:return_consumed_capacity
    -> unit
    -> get_item_request

  type get_item_response =
    { item : item option
    ; consumed_capacity : consumed_capacity option
    }

  type query_request =
    { table_name : string
    ; key_condition_expression : string
    ; expression_attribute_names : expression_attribute_names option
    ; expression_attribute_values : expression_attribute_values option
    ; filter_expression : string option
    ; limit : int option
    ; consistent_read : bool option
    ; index_name : string option
    ; exclusive_start_key : string_attr_value_list option
    ; return_consumed_capacity : return_consumed_capacity option
    }

  val query_request_to_yojson : query_request -> Yojson.Safe.t

  val make_query_request :
       table_name:string
    -> key_condition_expression:string
    -> ?expression_attribute_names:expression_attribute_names
    -> ?expression_attribute_values:expression_attribute_values
    -> ?filter_expression:string
    -> ?limit:int
    -> ?consistent_read:bool
    -> ?index_name:string
    -> ?exclusive_start_key:string_attr_value_list
    -> ?return_consumed_capacity:return_consumed_capacity
    -> unit
    -> query_request

  type query_response =
    { consumed_capacity : consumed_capacity option
    ; count : int
    ; items : item list
    ; last_evaluated_key : string_attr_value_list option
    ; scanned_count : int
    }

  type batch_write_item_request_raw =
    { request_items : request_items
    ; return_consumed_capacity : return_consumed_capacity option
    }

  val make_batch_write_item_request_raw :
       request_items:request_items
    -> ?return_consumed_capacity:return_consumed_capacity
    -> unit
    -> batch_write_item_request_raw

  type batch_write_item_request_request_items =
    | Put of item list
    | Delete of prim_and_range_key list

  type batch_write_item_request =
    (string * batch_write_item_request_request_items) list

  type batch_write_item_response =
    { consumed_capacity : consumed_capacity list option }

  type update_item_request =
    { table_name : string
    ; key : prim_and_range_key
    ; update_expression : string option
    ; condition_expression : string option
    ; expression_attribute_names : expression_attribute_names option
    ; expression_attribute_values : expression_attribute_values option
    ; return_consumed_capacity : return_consumed_capacity option
    ; return_values : return_values option
    }

  val make_update_item_request :
       table_name:string
    -> key:prim_and_range_key
    -> ?update_expression:string
    -> ?condition_expression:string
    -> ?expression_attribute_names:expression_attribute_names
    -> ?expression_attribute_values:expression_attribute_values
    -> ?return_consumed_capacity:return_consumed_capacity
    -> ?return_values:return_values
    -> unit
    -> update_item_request

  type update_item_response =
    { attributes : string_attr_value_list option
    ; consumed_capacity : consumed_capacity option
    }

  type batch_get_item_request =
    { request_items : batch_get_item_request_items
    ; return_consumed_capacity : return_consumed_capacity option
    }

  val make_batch_get_item_request :
       request_items:batch_get_item_request_items
    -> ?return_consumed_capacity:return_consumed_capacity
    -> unit
    -> batch_get_item_request

  type batch_get_item_response =
    { consumed_capacity : consumed_capacity list option
    ; responses : batch_get_item_response_responses
    ; unprocessed_keys : batch_get_item_request_items
    }

  type request =
    | List_tables
    | Get_item of get_item_request
    | Query of query_request
    | Batch_write_item of batch_write_item_request_raw
    | Update_item of update_item_request
    | Batch_get_item of batch_get_item_request
end

module Api : sig
  type config =
    { region : string
    ; access_key : string
    ; secret_key : string
    }

  type config'

  val get_config_exn :
    < fs : #Eio.Fs.dir Eio.Path.t ; .. > -> which:string -> config

  val run :
       < clock : Eio.Time.clock
       ; net : #Eio.Net.t
       ; secure_random : Eio.Flow.source
       ; .. >
    -> ?config:config
    -> (Tls_eio.t -> config' -> 'a)
    -> 'a

  val run2 : ?config:config -> (Tls_eio.t -> config' -> 'a -> 'b) -> 'a -> 'b

  val list_tables :
    Tls_eio.t -> config' -> 'a -> (Type.list_tables_response, string) result

  val get_item :
       Tls_eio.t
    -> config'
    -> Type.get_item_request
    -> (Type.get_item_response, string) result

  val query :
       Tls_eio.t
    -> config'
    -> Type.query_request
    -> (Type.query_response, string) result

  val batch_write_item :
       Tls_eio.t
    -> config'
    -> Type.batch_write_item_request
    -> (Type.batch_write_item_response list, string) result

  val update_item :
       Tls_eio.t
    -> config'
    -> Type.update_item_request
    -> (Type.update_item_response, string) result

  val batch_get_item :
       Tls_eio.t
    -> config'
    -> Type.batch_get_item_request
    -> (Type.batch_get_item_response, string) result
end
