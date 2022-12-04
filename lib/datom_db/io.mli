type 'a t

type 'a io = 'a t

val list_tables : (Dynamodb.Type.list_tables_response, Yojson.Safe.t) result t

val get_item :
     Dynamodb.Type.get_item_request
  -> (Dynamodb.Type.get_item_response, Yojson.Safe.t) result t

val query :
     Dynamodb.Type.query_request
  -> (Dynamodb.Type.query_response, Yojson.Safe.t) result t

val batch_write_item :
     Dynamodb.Type.batch_write_item_request
  -> (Dynamodb.Type.batch_write_item_response list, Yojson.Safe.t) result t

val update_item :
     Dynamodb.Type.update_item_request
  -> (Dynamodb.Type.update_item_response, Yojson.Safe.t) result t

module Monad : Monad.S with type 'a t = 'a io
