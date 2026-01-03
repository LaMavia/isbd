#include <caml/bigarray.h>
#include <caml/fail.h>
#include <caml/mlvalues.h>
#include <endian.h>
#include <stdint.h>
#include <stdlib.h>

/* don't use CAMLparam1/CAMLreturn because bigarray is not heap-allocated
 * https://ocaml.org/manual/5.4/intfc.html#s%3AC-Bigarrays
 * */
CAMLprim value caml_create_big_bytes(intptr_t length) {
  if (length < 0) {
    caml_invalid_argument("negative big_bytes length");
  }

  uint8_t *result = (uint8_t *)malloc(sizeof(uint8_t) * length);
  if (result == NULL) {
    caml_raise_out_of_memory();
  }

  intptr_t dims[] = {length};

  return caml_ba_alloc(CAML_BA_UINT8 | CAML_BA_C_LAYOUT | CAML_BA_EXTERNAL, 1,
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
          CAML_BA_EXTERNAL &&
      b->data != NULL) {
    // printf("Freeing %p, length=%ld\n", b, b->dim[0]);
    free(b->data);
    b->data = NULL;
  }
}

CAMLprim uint8_t caml_get_uint8(value ba_v, intptr_t offset) {
  struct caml_ba_array *b = Caml_ba_array_val(ba_v);

  return *((uint8_t *)b->data + offset);
}

CAMLprim value caml_get_uint8_byte(value ba_v, value offset_v) {
  return caml_get_uint8(ba_v, Int_val(offset_v));
}

inline int64_t get_int64_be(struct caml_ba_array *b, intptr_t offset) {
  return be64toh(*((int64_t *)((uint8_t *)b->data + offset)));
}

CAMLprim int64_t caml_get_int64_be(value ba_v, intptr_t offset) {
  return get_int64_be(Caml_ba_array_val(ba_v), offset);
}

CAMLprim int64_t caml_get_int64_be_byte(value ba_v, value offset_v) {
  return caml_get_int64_be(ba_v, Int_val(offset_v));
}

inline void set_int64_be(struct caml_ba_array *b, intptr_t offset,
                         int64_t val) {
  *((int64_t *)((uint8_t *)b->data + offset)) = htobe64(val);
}

CAMLexport void caml_set_int64_be(value ba_v, intptr_t offset, int64_t val) {
  set_int64_be(Caml_ba_array_val(ba_v), offset, val);
}

CAMLexport void caml_set_int64_be_byte(value ba_v, value offset_v,
                                       value val_v) {
  return caml_set_int64_be(ba_v, Int_val(offset_v), Int64_val(val_v));
}

#define VLE_NEG_SIGN_MASK 0b01000000
#define VLE_POS_SIGN_MASK 0b0
#define VLE_HEAD_MAX_VAL 63L
#define VLE_CONTINUE_MASK 0b10000000U
#define VLE_HEAD_VAL_MASK 0b00111111L
#define VLE_TAIL_MAX_VAL 127L
#define VLE_TAIL_VAL_MASK 0b01111111L
#define VLE_HEAD_VAL_LENGTH 6
#define VLE_TAIL_VAL_LENGTH 7

intptr_t encode_vle(struct caml_ba_array *in_ba, struct caml_ba_array *out_ba,
                    intptr_t input_length, intptr_t output_position) {
  int64_t last_value = 0L;
  uint8_t *out_data = (uint8_t *)out_ba->data;

  for (intptr_t i = 0; i < input_length; i += sizeof(int64_t)) {
    int64_t current_value = get_int64_be(in_ba, i);
    int64_t val = current_value - last_value;
    uint8_t sign_mask;
    if (val < 0) {
      sign_mask = VLE_NEG_SIGN_MASK;
    } else {
      sign_mask = VLE_POS_SIGN_MASK;
    }

    last_value = current_value;
    val = llabs(val);

    uint8_t continue_mask = 0;
    if (val > VLE_HEAD_MAX_VAL) {
      continue_mask = VLE_CONTINUE_MASK;
    }

    uint8_t octet_val =
        continue_mask | sign_mask | (uint8_t)(val & VLE_HEAD_VAL_MASK);

    *((out_data + output_position++)) = octet_val;

    val >>= VLE_HEAD_VAL_LENGTH;
    while (continue_mask) {
      if (val <= VLE_TAIL_MAX_VAL) {
        continue_mask = 0;
      }

      octet_val = continue_mask | (uint8_t)(val & VLE_TAIL_VAL_MASK);
      *((out_data + output_position++)) = octet_val;

      val >>= VLE_TAIL_VAL_LENGTH;
    }
  }

  return output_position;
}

CAMLprim intptr_t caml_encode_vle(value in_ba_v, value out_ba_v,
                                  intptr_t input_length,
                                  intptr_t output_position) {
  return encode_vle(Caml_ba_array_val(in_ba_v), Caml_ba_array_val(out_ba_v),
                    input_length, output_position);
}

CAMLprim value caml_encode_vle_byte(value in_ba_v, value out_ba_v,
                                    value input_length_v,
                                    value output_position_v) {
  return Val_int(caml_encode_vle(in_ba_v, out_ba_v, Int_val(input_length_v),
                                 Int_val(output_position_v)));
}

intptr_t count_vle_elements(struct caml_ba_array *in_ba,
                            intptr_t input_length) {
  int64_t count = 0L;
  uint8_t *in_data = (uint8_t *)(in_ba->data);
  for (intptr_t i = 0; i < input_length; i++) {
    if ((*(in_data + i) & VLE_CONTINUE_MASK) == 0) {
      count++;
    }
  }

  return count;
}

CAMLprim intptr_t caml_count_vle_elements(value in_ba_v,
                                          intptr_t input_length) {
  return count_vle_elements(Caml_ba_array_val(in_ba_v), input_length);
}

CAMLprim value caml_count_vle_elements_byte(value in_ba_v,
                                            value input_length_v) {
  return Val_int(caml_count_vle_elements(in_ba_v, Int_val(input_length_v)));
}

intptr_t decode_vle(struct caml_ba_array *in_ba, struct caml_ba_array *out_ba,
                    intptr_t input_length, intptr_t output_position) {
  int64_t last_value = 0L;
  uint8_t *in_data = (uint8_t *)in_ba->data;

  for (intptr_t i = 0; i < input_length;) {
    int64_t shift = VLE_HEAD_VAL_LENGTH;
    int64_t acc = 0L;
    uint8_t octet = *(in_data + i++);
    int64_t sign = 1L;
    if (octet & VLE_NEG_SIGN_MASK) {
      sign = -1L;
    }

    acc = (int64_t)(octet & VLE_HEAD_VAL_MASK);
    while (octet & VLE_CONTINUE_MASK) {
      octet = *(in_data + i++);
      acc |= (int64_t)(octet & VLE_TAIL_VAL_MASK) << shift;
      shift += VLE_TAIL_VAL_LENGTH;
    }

    acc *= sign;
    last_value += acc;
    set_int64_be(out_ba, output_position, last_value);
    output_position += sizeof(int64_t);
  }

  return output_position;
}

CAMLprim intptr_t caml_decode_vle(value in_ba_v, value out_ba_v,
                                  intptr_t input_length,
                                  intptr_t output_position) {
  return decode_vle(Caml_ba_array_val(in_ba_v), Caml_ba_array_val(out_ba_v),
                    input_length, output_position);
}

CAMLprim value caml_decode_vle_byte(value in_ba_v, value out_ba_v,
                                    value input_length_v,
                                    value output_position_v) {
  return Val_int(caml_decode_vle(in_ba_v, out_ba_v, Int_val(input_length_v),
                                 Int_val(output_position_v)));
}
