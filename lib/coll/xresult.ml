include Result

module type Error = sig
  type t
end

module MonadBasic (E : Error) = struct
  type 'a t = ('a, E.t) result

  let return x = Ok x

  let bind r ~f = bind r f

  let map r ~f = map f r
end

module MonadMake (E : Error) : Monad.S with type 'a t = ('a, E.t) result =
  Monad.Make (MonadBasic (E))
