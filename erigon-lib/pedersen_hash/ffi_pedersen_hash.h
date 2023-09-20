#ifndef STARKWARE_CRYPTO_FFI_PEDERSEN_HASH_H_
#define STARKWARE_CRYPTO_FFI_PEDERSEN_HASH_H_

int Hash(const char* in1, const char* in2, char* out);
int GoHash(const char* in1, const char* in2, char* out);

#endif  // STARKWARE_CRYPTO_FFI_PEDERSEN_HASH_H_
