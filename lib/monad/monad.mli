module type Basic = Monad_intf.Basic

module type S = Monad_intf.S

module Make (M : Basic) : Monad_intf.S with type 'a t = 'a M.t
[@@inlined always]
