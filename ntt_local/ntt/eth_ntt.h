#ifndef ETH_NTT_H
#define ETH_NTT_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Precompile entry points ──
 *
 * Take raw EVM calldata, return output_bytes.
 * Return 0 on success, negative on error:
 *   -1: input too short
 *   -2: invalid field parameters
 *   -3: unexpected input length
 *   -4: parameter overflow
 *
 * On success, *output_out and *output_len_out are set to a heap-allocated
 * buffer that the caller must free with eth_ntt_free_buffer().
 */

int32_t eth_ntt_fw_precompile(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

int32_t eth_ntt_inv_precompile(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

int32_t eth_ntt_vecmulmod_precompile(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

int32_t eth_ntt_vecaddmod_precompile(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

void eth_ntt_free_buffer(uint8_t *ptr, size_t len);

/* ── Fast direct API ──
 *
 * For callers who want to avoid precompile encoding overhead.
 * FastNttParams precomputes twiddle tables and is safe to share
 * across threads (read-only after creation).
 */

typedef struct FastNttParams FastNttParams;

FastNttParams *eth_ntt_fast_params_new(uint64_t q, size_t n, uint64_t psi);
void eth_ntt_fast_params_free(FastNttParams *params);
uint64_t eth_ntt_fast_params_q(const FastNttParams *params);
size_t eth_ntt_fast_params_n(const FastNttParams *params);
size_t eth_ntt_fast_params_coeff_bytes(const FastNttParams *params);

void eth_ntt_fw(
    const FastNttParams *params,
    const uint64_t *input, uint64_t *output, size_t n);

void eth_ntt_inv(
    const FastNttParams *params,
    const uint64_t *input, uint64_t *output, size_t n);

void eth_ntt_vec_mul_mod(
    const uint64_t *a, const uint64_t *b,
    uint64_t *output, size_t n, uint64_t q);

void eth_ntt_vec_add_mod(
    const uint64_t *a, const uint64_t *b,
    uint64_t *output, size_t n, uint64_t q);

/* Element-wise modular subtraction: result[i] = (a[i] - b[i]) mod q.
 * Same input format as VECMULMOD/VECADDMOD. */
int32_t eth_ntt_vecsubmod_precompile(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

/* ExpandA + matrix-vector multiply (ML-DSA / ML-KEM).
 * Expands A from rho via SHAKE128, then computes A × z in NTT domain.
 * Input: q(32 BE) | n(32 BE) | k(32 BE) | l(32 BE) | rho(32) | z(l*n*cb)
 * Output: k*n*cb bytes — result[i] = sum_j(A[i][j] * z[j]) mod q */
int32_t eth_ntt_expand_a_vecmul_precompile(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

/* Generic SHAKE-N (SHAKE128 / SHAKE256).
 * Input: security(32 BE) | output_len(32 BE) | data(var)
 * security must be 128 or 256. Returns output_len bytes. */
int32_t eth_ntt_shake(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

/* Falcon-512 verify: SHAKE256 HTP + NTT + VECMUL + INTT + norm check.
 * Input: s2(1024, 512×uint16 BE) | ntth(1024, 512×uint16 BE) | salt_msg(var)
 * Output: 32 bytes (0x00..01 valid, 0x00..00 invalid) */
int32_t eth_ntt_falcon_verify(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

/* ML-DSA-44 (Dilithium2) full verification.
 * Input: pk(1312) | sig(2420) | msg(var)
 * Output: 32 bytes (0x00..01 valid, 0x00..00 invalid) */
int32_t eth_ntt_dilithium_verify(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

/* Generalized LpNorm for any lattice-based signature.
 * Input: q(32 BE) | n(32 BE) | bound(32 BE) | cb(32 BE) | s1(n*cb) | s2(n*cb) | hashed(n*cb)
 * Computes centered L2: ||(hashed - s1) mod q||^2 + ||s2||^2 < bound
 * Output: 32 bytes (0x00..01 valid, 0x00..00 invalid) */
int32_t eth_ntt_lp_norm(
    const uint8_t *input, size_t input_len,
    uint8_t **output_out, size_t *output_len_out);

#ifdef __cplusplus
}
#endif

#endif /* ETH_NTT_H */
