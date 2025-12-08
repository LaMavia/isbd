let transpose rows cols a = Array.init_matrix cols rows @@ fun c r -> a.(r).(c)
