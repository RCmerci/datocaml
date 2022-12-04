include List
open Effect

module MonadBasic = struct
  type 'a t = 'a list

  let return x = [ x ]

  let bind l ~f = flatten (map f l)

  let map l ~f = map f l
end

module Monad = Monad.Make (MonadBasic)

module type Fold_left_with_stop_S = sig
  type acc_t

  val stop : acc_t -> 'a

  val fold_left_with_stop : (acc_t -> 'a -> acc_t) -> acc_t -> 'a list -> acc_t
end

module Fold_left_with_stop (Acc : sig
  type t
end) : Fold_left_with_stop_S with type acc_t = Acc.t = struct
  type acc_t = Acc.t

  type _ Effect.t += Stop : acc_t -> 'a Effect.t

  let stop acc = perform @@ Stop acc

  let fold_left_with_stop f acc l =
    Deep.try_with (fold_left f acc) l
      { effc =
          (fun (type b) (eff : b Effect.t) ->
            match eff with
            | Stop acc -> Some (fun _k -> acc)
            | _ -> None)
      }
end

let rec partition_by_n r l n =
  if n = 0 then (List.rev r, l)
  else
    match l with
    | [] -> (List.rev r, [])
    | x :: xs -> partition_by_n (x :: r) xs (n - 1)

let partition_by_n l n =
  assert (n > 0);
  partition_by_n [] l n

let rec partition_all_by_n r l n =
  if length l = 0 then rev r
  else
    let l1, l2 = partition_by_n l n in
    partition_all_by_n (l1 :: r) l2 n

let partition_all_by_n l n =
  assert (n > 0);
  partition_all_by_n [] l n
