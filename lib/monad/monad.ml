module type Basic = Monad_intf.Basic
module type S = Monad_intf.S

module Make (M : Basic) = struct
  include M

  module O = struct
    let ( >>= ) t f = bind t ~f

    let ( >>| ) t f = map t ~f

    let ( >>> ) a b = bind a ~f:(fun () -> b)

    let ( let+ ) t f = map t ~f

    let ( and+ ) x y =
      let open M in
      x >>= fun x ->
      y >>= fun y -> return (x, y)

    let ( let* ) t f = bind t ~f

    let ( and* ) = ( and+ )
  end
end
[@@inline always]
