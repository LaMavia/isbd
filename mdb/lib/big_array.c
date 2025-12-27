#include <caml/bigarray.h>
#include <caml/fail.h>
#include <caml/mlvalues.h>
#include <endian.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* don't use CAMLparam1/CAMLreturn because bigarray is not heap-allocated
 * https://ocaml.org/manual/5.4/intfc.html#s%3AC-Bigarrays
 * */
CAMLprim value caml_create_big_bytes(intptr_t length) {
  if (length < 0) {
    caml_invalid_argument("negative big_bytes length");
  }

  char *result = malloc(sizeof(char) * length);
  if (result == NULL) {
    caml_raise_out_of_memory();
  }

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

CAMLprim uint8_t caml_get_uint8(value ba_v, intptr_t offset) {
  struct caml_ba_array *b = Caml_ba_array_val(ba_v);

  return *((uint8_t *)(b->data + offset));
}

CAMLprim value caml_get_uint8_byte(value ba_v, value offset_v) {
  return caml_get_uint8(ba_v, Int_val(offset_v));
}

CAMLprim int64_t caml_get_int64_be(value ba_v, intptr_t offset) {
  struct caml_ba_array *b = Caml_ba_array_val(ba_v);

  return be64toh(*((int64_t *)(b->data + offset)));
}

CAMLprim int64_t caml_get_int64_be_byte(value ba_v, value offset_v) {
  return caml_get_int64_be(ba_v, Int_val(offset_v));
}

CAMLexport void caml_set_int64_be(value ba_v, intptr_t offset, int64_t val) {
  struct caml_ba_array *b = Caml_ba_array_val(ba_v);

  *((int64_t *)(b->data + offset)) = htobe64(val);
}

CAMLexport void caml_set_int64_be_byte(value ba_v, value offset_v,
                                       value val_v) {
  return caml_set_int64_be(ba_v, Int_val(offset_v), Int64_val(val_v));
}
