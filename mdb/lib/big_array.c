#include <caml/bigarray.h>
#include <caml/mlvalues.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* don't use CAMLparam1/CAMLreturn because bigarray is not heap-allocated
 * https://ocaml.org/manual/5.4/intfc.html#s%3AC-Bigarrays
 * */
CAMLprim value caml_create_big_bytes(intptr_t length) {
  char *result = malloc(sizeof(char) * length);
  CAMLassert(result != NULL);

  intptr_t dims[] = {length};

  return caml_ba_alloc(CAML_BA_CHAR | CAML_BA_C_LAYOUT | CAML_BA_EXTERNAL, 1,
                       result, dims);
}

CAMLprim value caml_create_big_bytes_byte(value length) {
  return caml_create_big_bytes(Int64_val(length));
}

/* https://github.com/ocaml/ocaml/blob/c7574991dc3704c901e273f57f460c3a03b811dc/runtime/bigarray.c#L283
 */
CAMLexport void caml_free_big_bytes(value v) {
  struct caml_ba_array *b = Caml_ba_array_val(v);
  if ((enum caml_ba_managed)(b->flags & CAML_BA_MANAGED_MASK) ==
      CAML_BA_EXTERNAL) {
    printf("Freeing %p, length=%ld\n", b, b->dim[0]);
    free(b->data);
  }
}
