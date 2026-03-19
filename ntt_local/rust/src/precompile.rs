use num_bigint::BigUint;
use num_traits::Zero;
use thiserror::Error;

use crate::fast::{self, FastNttParams};
use crate::field::FieldParams;
use crate::ntt;

const WORD: usize = 32;

#[derive(Debug, Error)]
pub enum PrecompileError {
    #[error("input too short")]
    InputTooShort,
    #[error("invalid field parameters: {0}")]
    InvalidParams(&'static str),
    #[error("unexpected input length")]
    BadLength,
    #[error("parameter overflow: {0}")]
    Overflow(&'static str),
}

/// Read a 32-byte big-endian word as a BigUint and advance the offset.
fn read_word(data: &[u8], offset: &mut usize) -> Result<BigUint, PrecompileError> {
    if *offset + WORD > data.len() {
        return Err(PrecompileError::InputTooShort);
    }
    let val = BigUint::from_bytes_be(&data[*offset..*offset + WORD]);
    *offset += WORD;
    Ok(val)
}

/// Read a 32-byte big-endian word, returning it as usize.
/// Rejects values that exceed the input length (impossible to be valid).
fn read_word_usize(data: &[u8], offset: &mut usize) -> Result<usize, PrecompileError> {
    let val = read_word(data, offset)?;
    if val.bits() > 64 {
        return Err(PrecompileError::Overflow("value exceeds 64 bits"));
    }
    let bytes = val.to_bytes_be();
    let mut buf = [0u8; 8];
    let start = 8usize.saturating_sub(bytes.len());
    buf[start..start + bytes.len()].copy_from_slice(&bytes);
    let v = u64::from_be_bytes(buf) as usize;
    if v > data.len() {
        return Err(PrecompileError::Overflow("parameter exceeds input size"));
    }
    Ok(v)
}

/// Read `len` raw bytes and decode as big-endian BigUint.
fn read_biguint(data: &[u8], offset: &mut usize, len: usize) -> Result<BigUint, PrecompileError> {
    if len > data.len() || *offset > data.len() - len {
        return Err(PrecompileError::InputTooShort);
    }
    let val = BigUint::from_bytes_be(&data[*offset..*offset + len]);
    *offset += len;
    Ok(val)
}

/// Encode a BigUint as exactly `byte_len` bytes big-endian (zero-padded on the left).
fn encode_biguint(val: &BigUint, byte_len: usize) -> Vec<u8> {
    let bytes = val.to_bytes_be();
    let mut padded = vec![0u8; byte_len];
    let start = byte_len.saturating_sub(bytes.len());
    let copy_len = bytes.len().min(byte_len);
    padded[start..start + copy_len].copy_from_slice(&bytes[bytes.len() - copy_len..]);
    padded
}

/// Encode a usize as a 32-byte big-endian word.
fn encode_word(val: usize) -> [u8; WORD] {
    let mut buf = [0u8; WORD];
    buf[WORD - 8..].copy_from_slice(&(val as u64).to_be_bytes());
    buf
}

fn decode_vector(
    data: &[u8],
    offset: &mut usize,
    n: usize,
    coeff_bytes: usize,
) -> Result<Vec<BigUint>, PrecompileError> {
    let total = n.checked_mul(coeff_bytes)
        .ok_or(PrecompileError::Overflow("n * coeff_bytes overflow"))?;
    if total > data.len() || *offset > data.len() - total {
        return Err(PrecompileError::InputTooShort);
    }
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        v.push(read_biguint(data, offset, coeff_bytes)?);
    }
    Ok(v)
}

fn encode_vector(v: &[BigUint], coeff_bytes: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * coeff_bytes);
    for val in v {
        out.extend_from_slice(&encode_biguint(val, coeff_bytes));
    }
    out
}

/// Read `len` big-endian bytes as a u64. Caller must ensure len <= 8.
fn bytes_to_u64(data: &[u8]) -> u64 {
    debug_assert!(data.len() <= 8);
    let mut buf = [0u8; 8];
    buf[8 - data.len()..].copy_from_slice(data);
    u64::from_be_bytes(buf)
}

/// Encode u64 values as big-endian byte vectors.
fn encode_vector_u64(v: &[u64], coeff_bytes: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * coeff_bytes);
    for &val in v {
        let be = val.to_be_bytes();
        out.extend_from_slice(&be[8 - coeff_bytes..]);
    }
    out
}

/// Try to decode NTT calldata using u64 fast path.
/// Layout: n(32) | q(32) | psi(32) | coeffs(n × cb)
/// Returns `Ok(None)` if q >= 2^63 (fall back to BigUint).
fn try_decode_ntt_fast(
    input: &[u8],
) -> Result<Option<(FastNttParams, Vec<u64>)>, PrecompileError> {
    if input.len() < 3 * WORD {
        return Err(PrecompileError::InputTooShort);
    }
    let mut offset = 0;
    let n = read_word_usize(input, &mut offset)?;
    let q_big = read_word(input, &mut offset)?;
    let psi_big = read_word(input, &mut offset)?;

    // Fast path: q and psi must fit in u64
    if q_big.bits() > 63 || psi_big.bits() > 63 {
        return Ok(None);
    }
    let q = q_big.iter_u64_digits().next().unwrap_or(0);
    let psi = psi_big.iter_u64_digits().next().unwrap_or(0);
    if q == 0 {
        return Ok(None);
    }

    let fast = FastNttParams::new(q, n, psi).map_err(PrecompileError::InvalidParams)?;
    let cb = fast.coeff_bytes;

    let total = n.checked_mul(cb).ok_or(PrecompileError::Overflow("n * coeff_bytes overflow"))?;
    if offset + total != input.len() {
        return Err(PrecompileError::BadLength);
    }

    let mut a = Vec::with_capacity(n);
    for _ in 0..n {
        a.push(bytes_to_u64(&input[offset..offset + cb]));
        offset += cb;
    }

    Ok(Some((fast, a)))
}

/// Try to decode vec-op calldata using u64 fast path.
/// Layout: n(32) | q(32) | a(n × cb) | b(n × cb)
fn try_decode_vec_fast(
    input: &[u8],
) -> Result<Option<(u64, usize, Vec<u64>, Vec<u64>)>, PrecompileError> {
    if input.len() < 2 * WORD {
        return Err(PrecompileError::InputTooShort);
    }
    let mut offset = 0;
    let n = read_word_usize(input, &mut offset)?;
    let q_big = read_word(input, &mut offset)?;

    if q_big.bits() > 63 {
        return Ok(None);
    }
    let q = q_big.iter_u64_digits().next().unwrap_or(0);
    if q == 0 {
        return Ok(None);
    }

    let q_bits = 64 - q.leading_zeros();
    let cb = (q_bits as usize + 7) / 8;
    let expected = n.checked_mul(cb).and_then(|v| v.checked_mul(2))
        .ok_or(PrecompileError::Overflow("n * coeff_bytes overflow"))?;

    if offset + expected != input.len() {
        return Err(PrecompileError::BadLength);
    }

    let mut a = Vec::with_capacity(n);
    for _ in 0..n {
        a.push(bytes_to_u64(&input[offset..offset + cb]));
        offset += cb;
    }
    let mut b = Vec::with_capacity(n);
    for _ in 0..n {
        b.push(bytes_to_u64(&input[offset..offset + cb]));
        offset += cb;
    }

    Ok(Some((q, n, a, b)))
}

/// Decode calldata for NTT_FW / NTT_INV.
///
/// Layout: `n(32) | q(32) | psi(32) | coeffs(n × cb)`
fn decode_ntt_input(input: &[u8]) -> Result<(FieldParams, Vec<BigUint>), PrecompileError> {
    let mut offset = 0;
    let n = read_word_usize(input, &mut offset)?;
    let q = read_word(input, &mut offset)?;
    let psi = read_word(input, &mut offset)?;

    let params = FieldParams::new(q, n, psi).map_err(PrecompileError::InvalidParams)?;
    let coeff_bytes = params.coeff_byte_len();
    let a = decode_vector(input, &mut offset, n, coeff_bytes)?;

    if offset != input.len() {
        return Err(PrecompileError::BadLength);
    }

    Ok((params, a))
}

/// Decode calldata for NTT_VECMULMOD / NTT_VECADDMOD.
///
/// Layout: `n(32) | q(32) | a(n × cb) | b(n × cb)`
fn decode_vec_input(
    input: &[u8],
) -> Result<(BigUint, usize, Vec<BigUint>, Vec<BigUint>), PrecompileError> {
    let mut offset = 0;
    let n = read_word_usize(input, &mut offset)?;
    let q = read_word(input, &mut offset)?;
    if q.is_zero() {
        return Err(PrecompileError::InvalidParams("q must be nonzero"));
    }

    let coeff_bytes = (q.bits() as usize + 7) / 8;
    let a = decode_vector(input, &mut offset, n, coeff_bytes)?;
    let b = decode_vector(input, &mut offset, n, coeff_bytes)?;

    if offset != input.len() {
        return Err(PrecompileError::BadLength);
    }

    Ok((q, n, a, b))
}

// ─── Public precompile entry points ───

/// Execute `NTT_FW` precompile.
pub fn ntt_fw_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if let Some((fast, a)) = try_decode_ntt_fast(input)? {
        let result = fast::ntt_fw_fast(&a, &fast);
        return Ok(encode_vector_u64(&result, fast.coeff_bytes));
    }
    let (params, a) = decode_ntt_input(input)?;
    let coeff_bytes = params.coeff_byte_len();
    let result = ntt::ntt_fw(&a, &params);
    Ok(encode_vector(&result, coeff_bytes))
}

/// Execute `NTT_INV` precompile.
pub fn ntt_inv_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if let Some((fast, a)) = try_decode_ntt_fast(input)? {
        let result = fast::ntt_inv_fast(&a, &fast);
        return Ok(encode_vector_u64(&result, fast.coeff_bytes));
    }
    let (params, a) = decode_ntt_input(input)?;
    let coeff_bytes = params.coeff_byte_len();
    let result = ntt::ntt_inv(&a, &params);
    Ok(encode_vector(&result, coeff_bytes))
}

/// Execute `NTT_VECMULMOD` precompile.
pub fn ntt_vecmulmod_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if let Some((_q, _n, a, b)) = try_decode_vec_fast(input)? {
        let q_val = _q;
        let q_bits = 64 - q_val.leading_zeros();
        let cb = (q_bits as usize + 7) / 8;
        let result = fast::vec_mul_mod_fast(&a, &b, q_val);
        return Ok(encode_vector_u64(&result, cb));
    }
    let (q, _n, a, b) = decode_vec_input(input)?;
    let coeff_bytes = (q.bits() as usize + 7) / 8;
    let result = ntt::vec_mul_mod(&a, &b, &q);
    Ok(encode_vector(&result, coeff_bytes))
}

/// Execute `NTT_VECADDMOD` precompile.
pub fn ntt_vecaddmod_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if let Some((_q, _n, a, b)) = try_decode_vec_fast(input)? {
        let q_val = _q;
        let q_bits = 64 - q_val.leading_zeros();
        let cb = (q_bits as usize + 7) / 8;
        let result = fast::vec_add_mod_fast(&a, &b, q_val);
        return Ok(encode_vector_u64(&result, cb));
    }
    let (q, _n, a, b) = decode_vec_input(input)?;
    let coeff_bytes = (q.bits() as usize + 7) / 8;
    let result = ntt::vec_add_mod(&a, &b, &q);
    Ok(encode_vector(&result, coeff_bytes))
}

/// Execute `NTT_VECSUBMOD` precompile.
/// Same format as VECADDMOD but computes `result[i] = (a[i] - b[i]) mod q`.
pub fn ntt_vecsubmod_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if let Some((q_val, _n, a, b)) = try_decode_vec_fast(input)? {
        let q_bits = 64 - q_val.leading_zeros();
        let cb = (q_bits as usize + 7) / 8;
        let result: Vec<u64> = a.iter().zip(b.iter())
            .map(|(&ai, &bi)| if ai >= bi { ai - bi } else { q_val + ai - bi })
            .collect();
        return Ok(encode_vector_u64(&result, cb));
    }
    let (q, _n, a, b) = decode_vec_input(input)?;
    let coeff_bytes = (q.bits() as usize + 7) / 8;
    let result: Vec<BigUint> = a.iter().zip(b.iter())
        .map(|(ai, bi)| if ai >= bi { (ai - bi) % &q } else { &q - ((bi - ai) % &q) })
        .collect();
    Ok(encode_vector(&result, coeff_bytes))
}

/// Execute `EXPAND_A_VECMUL` precompile.
///
/// Expands matrix A from seed `rho` via SHAKE128 (ExpandA from ML-DSA/ML-KEM),
/// then computes the matrix-vector product A × z in the NTT domain.
///
/// Input: `q(32) | n(32) | k(32) | l(32) | rho(32) | z(l×n×cb)`
/// Output: `k×n×cb` bytes — result[i] = sum_j(A[i][j] * z[j]) mod q.
///
/// ExpandA: for each (i,j), A[i][j] = RejNTTPoly(SHAKE128(rho || j || i)).
/// Rejection sampling: read 3 bytes LE, mask to 23 bits, accept if < q.
/// Works for ML-DSA (q=8380417) and ML-KEM (q=3329).
pub fn expand_a_vecmul_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if input.len() < 5 * WORD {
        return Err(PrecompileError::InputTooShort);
    }
    let mut offset = 0;
    let q_big = read_word(input, &mut offset)?;
    let n = read_word_usize(input, &mut offset)?;
    let k = read_word_usize(input, &mut offset)?;
    let l = read_word_usize(input, &mut offset)?;

    if q_big.bits() > 63 || k == 0 || l == 0 || n == 0 {
        return Err(PrecompileError::InvalidParams("invalid expand_a_vecmul params"));
    }
    let q = q_big.iter_u64_digits().next().unwrap_or(0);
    if q == 0 || k > 8 || l > 8 || n > 1024 {
        return Err(PrecompileError::InvalidParams("params out of range"));
    }
    let q_bits = 64 - q.leading_zeros();
    let cb = (q_bits as usize + 7) / 8;

    // Read rho (32 bytes from a 32-byte word — may have leading zeros)
    let rho = &input[offset..offset + WORD];
    offset += WORD;

    // Read z[l] polynomials
    let z_total = l.checked_mul(n).and_then(|ln| ln.checked_mul(cb))
        .ok_or(PrecompileError::Overflow("l*n*cb overflow"))?;
    if offset + z_total != input.len() {
        return Err(PrecompileError::BadLength);
    }
    let mut z_polys: Vec<Vec<u64>> = Vec::with_capacity(l);
    for _ in 0..l {
        let mut poly = Vec::with_capacity(n);
        for _ in 0..n {
            poly.push(bytes_to_u64(&input[offset..offset + cb]));
            offset += cb;
        }
        z_polys.push(poly);
    }

    // ExpandA and multiply
    let mut out = Vec::with_capacity(k * n * cb);
    for i in 0..k {
        let mut acc: Option<Vec<u64>> = None;
        for j in 0..l {
            // ExpandA: A[i][j] = RejNTTPoly(SHAKE128(rho || j || i))
            let mut seed = Vec::with_capacity(34);
            seed.extend_from_slice(rho);
            seed.push(j as u8);
            seed.push(i as u8);
            let mut xof = vec![0u8; 840];
            shake_n(128, &seed, &mut xof);

            let mut a_poly = Vec::with_capacity(n);
            let mut p = 0;
            while a_poly.len() < n {
                if p + 2 >= xof.len() { break; }
                let val = xof[p] as u64
                    | ((xof[p + 1] as u64) << 8)
                    | (((xof[p + 2] & 0x7F) as u64) << 16);
                p += 3;
                if val < q {
                    a_poly.push(val);
                }
            }
            if a_poly.len() < n {
                return Err(PrecompileError::InvalidParams("ExpandA: insufficient XOF output"));
            }

            // Multiply and accumulate
            let prod = fast::vec_mul_mod_fast(&a_poly, &z_polys[j], q);
            acc = Some(match acc {
                None => prod,
                Some(a) => fast::vec_add_mod_fast(&a, &prod, q),
            });
        }
        out.extend_from_slice(&encode_vector_u64(&acc.unwrap(), cb));
    }
    Ok(out)
}

/// Read a 32-byte big-endian word as a plain usize (no input-size validation).
fn read_word_usize_raw(data: &[u8], offset: &mut usize) -> Result<usize, PrecompileError> {
    let val = read_word(data, offset)?;
    if val.bits() > 64 {
        return Err(PrecompileError::Overflow("value exceeds 64 bits"));
    }
    let bytes = val.to_bytes_be();
    let mut buf = [0u8; 8];
    let start = 8usize.saturating_sub(bytes.len());
    buf[start..start + bytes.len()].copy_from_slice(&bytes);
    Ok(u64::from_be_bytes(buf) as usize)
}

/// SHAKE-N sponge using raw Keccak-f[1600].
///
/// Accepts any security level N in [1, 256].
/// capacity = ceil(2N / 8) bytes, rate = 200 - capacity bytes.
/// Domain separator: 0x1F (SHAKE), padding: pad10*1.
pub fn shake_n(n: usize, data: &[u8], output: &mut [u8]) {
    let capacity_bytes = (2 * n + 7) / 8;
    let rate = 200 - capacity_bytes;
    let mut state = [0u64; 25];

    // Absorb
    let mut pos = 0;
    for &byte in data {
        state[pos / 8] ^= (byte as u64) << (8 * (pos % 8));
        pos += 1;
        if pos == rate {
            keccak::f1600(&mut state);
            pos = 0;
        }
    }

    // Pad: SHAKE domain separator 0x1F + pad10*1
    state[pos / 8] ^= 0x1F_u64 << (8 * (pos % 8));
    state[(rate - 1) / 8] ^= 0x80_u64 << (8 * ((rate - 1) % 8));
    keccak::f1600(&mut state);

    // Squeeze
    let mut squeezed = 0;
    while squeezed < output.len() {
        let block = rate.min(output.len() - squeezed);
        for i in 0..block {
            output[squeezed + i] = (state[i / 8] >> (8 * (i % 8))) as u8;
        }
        squeezed += block;
        if squeezed < output.len() {
            keccak::f1600(&mut state);
        }
    }
}

/// Execute `SHAKE256` precompile.
///
/// Input: `output_len(32 BE) | data(var)`
///
/// Returns `output_len` bytes of SHAKE256(data).
pub fn shake_precompile(input: &[u8]) -> Result<Vec<u8>, PrecompileError> {
    if input.len() < WORD {
        return Err(PrecompileError::InputTooShort);
    }
    const MAX_OUTPUT: usize = 1 << 20; // 1 MB

    let mut offset = 0;
    let output_len = read_word_usize_raw(input, &mut offset)?;

    if output_len > MAX_OUTPUT {
        return Err(PrecompileError::Overflow("output_len exceeds 1 MB"));
    }

    let data = &input[offset..];
    let mut output = vec![0u8; output_len];
    shake_n(256, data, &mut output);
    Ok(output)
}

// ─── Calldata encoders (for building inputs from Rust) ───

/// Encode calldata for NTT_FW / NTT_INV.
/// Layout: n(32) | q(32) | psi(32) | coeffs(n × cb)
pub fn encode_ntt_input(params: &FieldParams, a: &[BigUint]) -> Vec<u8> {
    let coeff_bytes = params.coeff_byte_len();

    let mut out = Vec::new();
    out.extend_from_slice(&encode_word(params.n));
    out.extend_from_slice(&encode_biguint(&params.q, WORD));
    out.extend_from_slice(&encode_biguint(&params.psi, WORD));
    out.extend_from_slice(&encode_vector(a, coeff_bytes));
    out
}

/// Encode calldata for NTT_VECMULMOD / NTT_VECADDMOD.
/// Layout: n(32) | q(32) | a(n × cb) | b(n × cb)
pub fn encode_vec_input(q: &BigUint, n: usize, a: &[BigUint], b: &[BigUint]) -> Vec<u8> {
    let coeff_bytes = (q.bits() as usize + 7) / 8;

    let mut out = Vec::new();
    out.extend_from_slice(&encode_word(n));
    out.extend_from_slice(&encode_biguint(q, WORD));
    out.extend_from_slice(&encode_vector(a, coeff_bytes));
    out.extend_from_slice(&encode_vector(b, coeff_bytes));
    out
}

/// Helper: decode output vector from precompile return bytes.
pub fn decode_output(output: &[u8], n: usize, coeff_bytes: usize) -> Vec<BigUint> {
    assert_eq!(
        output.len(),
        n * coeff_bytes,
        "output length mismatch"
    );
    (0..n)
        .map(|i| {
            let start = i * coeff_bytes;
            BigUint::from_bytes_be(&output[start..start + coeff_bytes])
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::mod_pow;
    use num_traits::{One, Zero};

    fn small_params() -> FieldParams {
        let q = BigUint::from(17u64);
        let n: usize = 4;
        let psi = BigUint::from(9u64);
        FieldParams::new(q, n, psi).unwrap()
    }

    fn falcon_params() -> FieldParams {
        let q = BigUint::from(12289u64);
        let n: usize = 512;
        let g = BigUint::from(11u64);
        let exp = (&q - BigUint::one()) / BigUint::from(2u64 * n as u64);
        let psi = mod_pow(&g, &exp, &q);
        FieldParams::new(q, n, psi).unwrap()
    }

    #[test]
    fn test_encode_word_be() {
        let w = encode_word(512);
        assert_eq!(w.len(), 32);
        assert_eq!(&w[..24], &[0u8; 24]);
        assert_eq!(u64::from_be_bytes(w[24..32].try_into().unwrap()), 512);
    }

    #[test]
    fn test_encode_biguint_zero() {
        let z = encode_biguint(&BigUint::zero(), 4);
        assert_eq!(z, vec![0, 0, 0, 0]);
    }

    #[test]
    fn test_ntt_fw_precompile_roundtrip() {
        let params = small_params();
        let a: Vec<BigUint> = vec![1u64, 2, 3, 4]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let input = encode_ntt_input(&params, &a);
        let output = ntt_fw_precompile(&input).unwrap();

        let cb = params.coeff_byte_len();
        let ntt_a = decode_output(&output, params.n, cb);

        let inv_input = encode_ntt_input(&params, &ntt_a);
        let inv_output = ntt_inv_precompile(&inv_input).unwrap();
        let recovered = decode_output(&inv_output, params.n, cb);

        assert_eq!(a, recovered);
    }

    #[test]
    fn test_vecmulmod_precompile() {
        let q = BigUint::from(17u64);
        let n = 4;
        let a: Vec<BigUint> = vec![5u64, 10, 15, 3]
            .into_iter()
            .map(BigUint::from)
            .collect();
        let b: Vec<BigUint> = vec![4u64, 8, 3, 16]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let input = encode_vec_input(&q, n, &a, &b);
        let output = ntt_vecmulmod_precompile(&input).unwrap();

        let results = decode_output(&output, n, 1);
        assert_eq!(
            results,
            vec![
                BigUint::from(3u64),
                BigUint::from(12u64),
                BigUint::from(11u64),
                BigUint::from(14u64),
            ]
        );
    }

    #[test]
    fn test_vecaddmod_precompile() {
        let q = BigUint::from(17u64);
        let n = 4;
        let a: Vec<BigUint> = vec![5u64, 10, 15, 3]
            .into_iter()
            .map(BigUint::from)
            .collect();
        let b: Vec<BigUint> = vec![4u64, 8, 3, 16]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let input = encode_vec_input(&q, n, &a, &b);
        let output = ntt_vecaddmod_precompile(&input).unwrap();

        let results = decode_output(&output, n, 1);
        assert_eq!(
            results,
            vec![
                BigUint::from(9u64),
                BigUint::from(1u64),
                BigUint::from(1u64),
                BigUint::from(2u64),
            ]
        );
    }

    #[test]
    fn test_full_polymul_via_precompiles_falcon() {
        let params = falcon_params();
        let n = params.n;
        let q = &params.q;
        let cb = params.coeff_byte_len();

        let f: Vec<BigUint> = (0..n).map(|i| BigUint::from(i as u64)).collect();
        let g: Vec<BigUint> = (0..n).map(|i| BigUint::from((n - i) as u64)).collect();

        let fw_f_out = ntt_fw_precompile(&encode_ntt_input(&params, &f)).unwrap();
        let fw_g_out = ntt_fw_precompile(&encode_ntt_input(&params, &g)).unwrap();
        let ntt_f = decode_output(&fw_f_out, n, cb);
        let ntt_g = decode_output(&fw_g_out, n, cb);

        let mul_out = ntt_vecmulmod_precompile(&encode_vec_input(q, n, &ntt_f, &ntt_g)).unwrap();
        let ntt_product = decode_output(&mul_out, n, cb);

        let inv_out = ntt_inv_precompile(&encode_ntt_input(&params, &ntt_product)).unwrap();
        let product = decode_output(&inv_out, n, cb);

        let mut expected = vec![BigUint::from(0u64); n];
        for i in 0..n {
            for j in 0..n {
                let c = (&f[i] * &g[j]) % q;
                if i + j < n {
                    expected[i + j] = (&expected[i + j] + &c) % q;
                } else {
                    let idx = i + j - n;
                    expected[idx] = (q + &expected[idx] - &c) % q;
                }
            }
        }

        assert_eq!(product, expected);
    }

    #[test]
    fn test_calldata_is_all_big_endian() {
        // New layout: n(32) | q(32) | psi(32) | coeffs(n × cb)
        let params = small_params(); // q=17, n=4, psi=9
        let a: Vec<BigUint> = vec![1u64, 2, 3, 4]
            .into_iter()
            .map(BigUint::from)
            .collect();

        let input = encode_ntt_input(&params, &a);

        // n = 4 at word 0
        assert_eq!(input[31], 4);
        assert_eq!(&input[..31], &[0u8; 31]);

        // q = 17 at word 1
        assert_eq!(input[63], 17);

        // psi = 9 at word 2
        assert_eq!(input[95], 9);

        // coefficients start at byte 96, cb=1 for q=17
        assert_eq!(&input[96..100], &[1, 2, 3, 4]);
        assert_eq!(input.len(), 100); // 96 header + 4 coefficients
    }

    #[test]
    fn test_shake256_precompile() {
        // Format: output_len(32) | data
        let mut input = Vec::new();
        input.extend_from_slice(&encode_word(64));  // output_len
        input.extend_from_slice(b"test data");
        let output = shake_precompile(&input).unwrap();
        assert_eq!(output.len(), 64);

        // Deterministic
        let output2 = shake_precompile(&input).unwrap();
        assert_eq!(output, output2);

        // Matches sha3 crate
        use sha3::digest::{ExtendableOutput, Update, XofReader};
        let mut expected = [0u8; 64];
        let mut h = sha3::Shake256::default();
        h.update(b"test data");
        h.finalize_xof().read(&mut expected);
        assert_eq!(output, expected);
    }

    #[test]
    fn test_shake256_precompile_empty() {
        let mut input = Vec::new();
        input.extend_from_slice(&encode_word(0)); // output_len = 0
        let output = shake_precompile(&input).unwrap();
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_shake_n_matches_sha3_crate() {
        use sha3::digest::{ExtendableOutput, Update, XofReader};

        let data = b"test data for cross-check";

        // SHAKE128: our sponge must match sha3 crate
        let mut ours_128 = [0u8; 64];
        shake_n(128, data, &mut ours_128);
        let mut theirs_128 = [0u8; 64];
        let mut h = sha3::Shake128::default();
        h.update(data);
        h.finalize_xof().read(&mut theirs_128);
        assert_eq!(ours_128, theirs_128, "SHAKE-128 mismatch vs sha3 crate");

        // SHAKE256: our sponge must match sha3 crate
        let mut ours_256 = [0u8; 64];
        shake_n(256, data, &mut ours_256);
        let mut theirs_256 = [0u8; 64];
        let mut h = sha3::Shake256::default();
        h.update(data);
        h.finalize_xof().read(&mut theirs_256);
        assert_eq!(ours_256, theirs_256, "SHAKE-256 mismatch vs sha3 crate");

        // Different N → different output
        assert_ne!(ours_128, ours_256);
    }

    #[test]
    fn test_shake_n_arbitrary() {
        // Any N in [1, 256] should work and produce deterministic output
        for n in [1, 2, 4, 7, 16, 42, 100, 128, 200, 255, 256] {
            let mut out1 = [0u8; 32];
            let mut out2 = [0u8; 32];
            shake_n(n, b"hello", &mut out1);
            shake_n(n, b"hello", &mut out2);
            assert_eq!(out1, out2, "SHAKE-{n} not deterministic");
        }

        // Different N → different output (for most pairs)
        let mut out_1 = [0u8; 32];
        let mut out_128 = [0u8; 32];
        shake_n(1, b"hello", &mut out_1);
        shake_n(128, b"hello", &mut out_128);
        assert_ne!(out_1, out_128);
    }
}
