open Coll
open Dynamodb
open Dynamodb.Api

type state =
  { mutable consumed_capacity : float
  ; env :
      < clock : Eio.Time.clock
      ; net : Eio.Net.t
      ; secure_random : Eio.Flow.source >
  ; config : Api.config option
  }

type 'a t = state -> 'a

type 'a io = 'a t

module ResultO = Xresult.MonadMake (Yojson.Safe)
open ResultO.O

let update_consumed_capacity state
    (consumed_capacity : Type.consumed_capacity option) =
  (let open Xoption.Monad.O in
  let* consumed = consumed_capacity in
  let+ capacity_units = consumed.capacity_units in
  state.consumed_capacity <- state.consumed_capacity +. capacity_units)
  |> ignore

let list_tables state =
  run state.env ?config:state.config (fun conn config ->
      list_tables conn config ())

let get_item request state =
  let+ resp =
    run state.env ?config:state.config (fun conn config ->
        get_item conn config request)
  in
  update_consumed_capacity state resp.consumed_capacity;
  resp

let rec query request state =
  let* resp =
    run state.env ?config:state.config (fun conn config ->
        Api.query conn config request)
  in
  match resp.last_evaluated_key with
  | None -> ResultO.return resp
  | Some v ->
    let* (resp' : Type.query_response) =
      query { request with exclusive_start_key = Some v } state
    in
    let consumed_capacity : Type.consumed_capacity option =
      match (resp.consumed_capacity, resp'.consumed_capacity) with
      | Some v1, Some v2 ->
        let capacity_units =
          match (v1.capacity_units, v2.capacity_units) with
          | Some capacity_units1, Some capacity_units2 ->
            Some (capacity_units1 +. capacity_units2)
          | _ -> None
        in
        let table_name = v1.table_name in
        let read_capacity_units =
          match (v1.read_capacity_units, v2.read_capacity_units) with
          | Some read_capacity_units1, Some read_capacity_units2 ->
            Some (read_capacity_units1 +. read_capacity_units2)
          | _ -> None
        in
        let write_capacity_units =
          match (v1.write_capacity_units, v2.write_capacity_units) with
          | Some write_capacity_units1, Some write_capacity_units2 ->
            Some (write_capacity_units1 +. write_capacity_units2)
          | _ -> None
        in
        Some
          { capacity_units
          ; table_name
          ; read_capacity_units
          ; write_capacity_units
          }
      | _ -> None
    in
    update_consumed_capacity state consumed_capacity;
    ResultO.return
      ({ consumed_capacity
       ; count = resp.count + resp'.count
       ; items = List.concat [ resp.items; resp'.items ]
       ; last_evaluated_key = None
       ; scanned_count = resp.scanned_count + resp'.scanned_count
       }
        : Type.query_response)

let batch_write_item request state =
  let+ resp =
    run state.env ?config:state.config (fun conn config ->
        batch_write_item conn config request)
  in
  List.iter
    (fun (r : Type.batch_write_item_response) ->
      update_consumed_capacity state r.consumed_capacity)
    resp;
  resp

let update_item request state =
  let+ resp =
    run state.env ?config:state.config (fun conn config ->
        update_item conn config request)
  in
  update_consumed_capacity state resp.consumed_capacity;
  resp

module MonadBasic = struct
  type 'a t = 'a io

  let return x _state = x

  let bind x ~f state = (f (x state)) state

  let map x ~f state = f (x state)
end

module Monad = Monad.Make (MonadBasic)
