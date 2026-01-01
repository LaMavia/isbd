#include <caml/fail.h>
#include <caml/mlvalues.h>
#include <endian.h>
#include <stdint.h>

inline int compare_int64_t(int64_t a, int64_t b) {
  if (a < b)
    return -1;
  if (a > b)
    return 1;
  return 0;
}

CAMLprim int caml_compare_int64(int64_t a, int64_t b) {
  return compare_int64_t(a, b);
}

CAMLprim value caml_compare_int64_byte(value av, value bv) {
  return Val_int(compare_int64_t(Int64_val(av), Int64_val(bv)));
}
