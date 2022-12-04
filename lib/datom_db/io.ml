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

let query request state =
  let+ resp =
    run state.env ?config:state.config (fun conn config ->
        query conn config request)
  in
  update_consumed_capacity state resp.consumed_capacity;
  resp

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
