type t =
  | Id of string
  | Ref of (Attr.t * Value.t)
[@@deriving show]

let ensure_eid t (db_config : Db_config.t) : (string, string) result Io.t =
  match t with
  | Id s -> Io.Monad.return (Ok s)
  | Ref (k, _v) ->
    if not (Db_config.is_attr_unique db_config k) then
      Io.Monad.return
      @@ Error
           (Printf.sprintf "Lookup ref attribute should be marked as Unique: %s"
              (show t))
    else failwith "TODO: search by av"
