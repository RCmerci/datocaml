open Eio
open Cohttp_eio

type config =
  { region : string
  ; access_key : string
  ; secret_key : string
  }

type config' =
  { region : string
  ; access_key : string
  ; secret_key : string
  ; hostname : string
  }

let get_config_exn env ~which =
  match
    Fiber.pair
      (fun () -> Credential.get_credential env ~which)
      (fun () -> Credential.get_region env ~which)
  with
  | Some (access_key, secret_key), Some region ->
    { region; access_key; secret_key }
  | None, _ -> failwith "get access_key, secret_key failed"
  | _, None -> failwith "get region failed"

let default_config =
  lazy (Eio_main.run @@ fun env -> get_config_exn env ~which:"default")

let tls_config =
  Tls.Config.client ~authenticator:(fun ?ip:_ ~host:_ _certs -> Ok None) ()

let run env ?(config : config option) f =
  let config =
    match config with
    | Some v -> v
    | None -> Lazy.force default_config
  in
  Mirage_crypto_rng_eio.run (module Mirage_crypto_rng.Fortuna) env @@ fun () ->
  match Endpoints.endpoint_of "dynamodb" config.region with
  | None -> failwith "not found endpoint"
  | Some hostname ->
    let config' =
      { region = config.region
      ; access_key = config.access_key
      ; secret_key = config.secret_key
      ; hostname
      }
    in
    Net.with_tcp_connect ~host:hostname ~service:"https" env#net @@ fun conn ->
    let conn =
      Tls_eio.client_of_flow tls_config
        ?host:
          (Domain_name.of_string_exn hostname
          |> Domain_name.host |> Result.to_option)
        conn
    in
    f conn config'

let run2 ?config f request =
  Eio_main.run @@ fun env ->
  run env ?config @@ fun env config -> f env config request

let call_api conn config api_name request =
  let uri = Uri.of_string @@ "https://" ^ config.hostname ^ "/" in
  let body = Type.request_to_yojson request |> Yojson.Safe.to_string in
  let headers =
    [ ("Host", config.hostname)
    ; ("Accept-Encoding", "identity")
    ; ("Connection", "Keep-Alive")
    ; ("Content-Type", "application/x-amz-json-1.0")
    ; ("X-Amz-Target", api_name)
    ; ("Content-Length", string_of_int @@ String.length body)
    ]
  in
  let _, _, headers =
    Sign.sign_request ~access_key:config.access_key
      ~secret_key:config.secret_key ~service:"dynamodb" ~region:config.region
      (`POST, uri, headers, body)
  in
  let r =
    Client.post ~version:`HTTP_1_1
      ~headers:(Http.Header.of_list headers)
      ~body:(Body.Fixed body) ~conn
      (config.hostname, Some 443)
      "/"
  in
  Client.read_fixed r |> Yojson.Safe.from_string

(* apis *)

let list_tables conn config _request =
  let json =
    call_api conn config "DynamoDB_20120810.ListTables" Type.List_tables
  in
  match Type.list_tables_response_of_yojson json with
  | Ok v -> Ok v
  | Error _ -> Error (Yojson.Safe.to_string json)

let get_item conn config request =
  let json =
    call_api conn config "DynamoDB_20120810.GetItem" (Type.Get_item request)
  in
  match Type.get_item_response_of_yojson json with
  | Ok v -> Ok v
  | Error _ -> Error (Yojson.Safe.to_string json)

let query conn config request =
  let json =
    call_api conn config "DynamoDB_20120810.Query" (Type.Query request)
  in
  match Type.query_response_of_yojson json with
  | Ok v -> Ok v
  | Error _ -> Error (Yojson.Safe.to_string json)

let batch_write_item conn config request =
  let requests =
    request |> Type.batch_write_item_request_to_raw
    |> Type.partition_batch_write_item_request_raw ~n:25
  in
  let module X = Coll.Xlist.Fold_left_with_stop (struct
    type t = (Type.batch_write_item_response list, string) result
  end) in
  X.fold_left_with_stop
    (fun acc request ->
      let json =
        call_api conn config "DynamoDB_20120810.BatchWriteItem"
          (Type.Batch_write_item request)
      in
      match (Type.batch_write_item_response_of_yojson json, acc) with
      | Ok v, Ok acc' -> Ok (v :: acc')
      | Error _, _ -> X.stop @@ Error (Yojson.Safe.to_string json)
      | _ -> failwith "unreachable")
    (Ok []) requests

let update_item conn config request =
  let json =
    call_api conn config "DynamoDB_20120810.UpdateItem"
      (Type.Update_item request)
  in
  match Type.update_item_response_of_yojson json with
  | Ok v -> Ok v
  | Error _ -> Error (Yojson.Safe.to_string json)

let batch_get_item conn config request =
  let json =
    call_api conn config "DynamoDB_20120810.BatchGetItem"
      (Type.Batch_get_item request)
  in
  match Type.batch_get_item_response_of_yojson json with
  | Ok v -> Ok v
  | Error s -> Error (Printf.sprintf "%s:%s" s (Yojson.Safe.to_string json))

(* test *)

let batch_get_item_request : Type.batch_get_item_request =
  Type.make_batch_get_item_request
    ~request_items:
      [ ( "datocaml_1"
        , Type.make_batch_get_item_request_item
            ~keys:
              [ Type.make_prim_and_range_key
                  ~primary_key:
                    ("tenant+e", Type.S "test_tenant#JIFthagqSufOu2naackai")
                  ~range_key:("a", Type.S "attr-1") ()
              ]
            () )
      ]
    ~return_consumed_capacity:Type.TOTAL ()

(* "test_tenant" "JIFthagqSufOu2naackai", "attr-1", S "attr-1-s" *)
let get_item_request : Type.get_item_request =
  Type.make_get_item_request ~table_name:"datocaml_1"
    ~key:
      (Type.make_prim_and_range_key
         ~primary_key:("tenant+e", Type.S "test_tenant#JIFthagqSufOu2naackai")
         (* ~range_key:("a", Type.S "yyy") *) ())
    ()

let update_item_request : Type.update_item_request =
  Type.make_update_item_request ~table_name:"file-sync-s3-object-metadata"
    ~key:
      (Type.make_prim_and_range_key
         ~primary_key:("user-uuid+graph-uuid", S "test")
         ~range_key:("s3-key", S "test2") ())
    ~update_expression:"set attr1=:v"
    ~expression_attribute_values:[ (":v", S "updated1") ]
    ~return_values:Type.RETURN_VALUES_UPDATED_NEW ()

let batch_write_item_request : Type.batch_write_item_request =
  [ ( "file-sync-s3-object-metadata"
    , Put
        [ [ ("user-uuid+graph-uuid", S "test")
          ; ("s3-key", S "000123#tenant")
          ; ("attr1", S "attr11")
          ]
        ; [ ("user-uuid+graph-uuid", S "test")
          ; ("s3-key", S "000001#tenant2")
          ; ("attr1", S "attr111")
          ]
        ] )
  ]

let batch_write_item_request_raw : Type.batch_write_item_request_raw =
  Type.make_batch_write_item_request_raw
    ~request_items:
      [ ( "file-sync-s3-object-metadata"
        , [ Delete_request
              { key =
                  { primary_key = ("user-uuid+graph-uuid", Type.S "test")
                  ; range_key = Some ("s3-key", Type.S "test")
                  }
              }
          ] )
      ]
    ()

let query_request : Type.query_request =
  { table_name = "file-sync-s3-object-metadata"
  ; key_condition_expression = "#k1=:v1 and #k2 between :v21 and :v22"
  ; expression_attribute_names =
      Some [ ("#k1", "user-uuid+graph-uuid"); ("#k2", "s3-key") ]
  ; expression_attribute_values =
      Some
        [ (":v1", S "test"); (":v21", S "000123#"); (":v22", S "000123#uuuuu") ]
  ; filter_expression = None
  ; limit = None
  ; consistent_read = None
  ; index_name = None
  ; exclusive_start_key = None
  ; return_consumed_capacity = Some Type.TOTAL
  }

let run3 env ?(config : config option) f =
  let config =
    match config with
    | Some v -> v
    | None -> Lazy.force default_config
  in
  Mirage_crypto_rng_eio.run (module Mirage_crypto_rng.Fortuna) env @@ fun () ->
  match Endpoints.endpoint_of "dynamodb" config.region with
  | None -> failwith "not found endpoint"
  | Some hostname ->
    let config' =
      { region = config.region
      ; access_key = config.access_key
      ; secret_key = config.secret_key
      ; hostname
      }
    in
    Net.with_tcp_connect ~host:hostname ~service:"https" env#net @@ fun conn' ->
    let conn =
      Tls_eio.client_of_flow tls_config
        ?host:
          (Domain_name.of_string_exn hostname
          |> Domain_name.host |> Result.to_option)
        conn'
    in
    Time.sleep env#clock 100.;
    f conn config'

let f () =
  Eio_main.run @@ fun env ->
  run3 env (fun conn config -> list_tables conn config ())
