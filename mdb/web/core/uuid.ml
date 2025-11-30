type t = Uuidm.t

let v4 = Uuidm.v4_gen (Random.State.make_self_init ())
let to_string u = Uuidm.to_string u
