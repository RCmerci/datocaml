include List
open Effect

module type Acc_t = sig
  type t
end

module Fold_left_with_stop (Acc : Acc_t) = struct
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

let partition_by_n l = partition_by_n [] l

let rec partition_all_by_n r l n =
  if length l = 0 then rev r
  else
    let l1, l2 = partition_by_n l n in
    partition_all_by_n (l1 :: r) l2 n

let partition_all_by_n l n =
  assert (n > 0);
  partition_all_by_n [] l n
