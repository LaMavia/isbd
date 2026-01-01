let is_absolute p = String.starts_with ~prefix:Filename.dir_sep p
let ( // ) p q = if is_absolute q then q else Filename.concat p q
