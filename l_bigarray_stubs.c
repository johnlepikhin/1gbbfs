
#include <stdio.h>
#include <unistd.h>
#include <caml/mlvalues.h>
#include <caml/bigarray.h>

CAMLprim value
caml_read(value ml_fh, value ml_array, value ml_offset, value ml_length)
{
	int fh = Int_val (ml_fh);
	char *a = Data_bigarray_val(ml_array);
	int offset = Int_val (ml_offset);
	int length = Int_val (ml_length);
	int rd = 0;
	if (length > 0) {
		a = a+offset;
		rd = read (fh, a, length);
	}
	return Val_int (rd);
}

CAMLprim value
caml_write(value ml_fh, value ml_array, value ml_offset, value ml_length)
{
	int fh = Int_val (ml_fh);
	char *a = Data_bigarray_val(ml_array);
	int offset = Int_val (ml_offset);
	int length = Int_val (ml_length);
	int wr = 0;
	if (length > 0) {
		a = a+offset;
		wr = write (fh, a, length);
	}
	return Val_int (wr);
}

