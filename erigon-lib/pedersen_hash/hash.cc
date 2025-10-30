#include "hash.h"
#include "ffi_pedersen_hash.h"

int CHash(const char* in1, const char* in2, char* out) {	
	int r = GoHash(in1, in2, out);
	return r;
}

