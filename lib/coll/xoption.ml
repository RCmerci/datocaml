include Option

module MonadBasic = struct
  type 'a t = 'a option

  let return x = Some x

  let bind x ~f = bind x f

  let map x ~f = map f x
end

module Monad = Monad.Make (MonadBasic)
