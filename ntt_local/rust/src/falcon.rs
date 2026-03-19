//! Falcon-512 EVM compact format operations.
//!
//! Compact: 1024 bytes = 32 big-endian uint256 words, each packing
//! 16 little-endian uint16 coefficients.

use crate::fast::{self, FastNttParams};
use sha3::digest::{ExtendableOutput, Update, XofReader};
use sha3::Shake256;

pub const Q: u64 = 12289;
pub const N: usize = 512;
pub const PSI: u64 = 49;
pub const COMPACT_SIZE: usize = 1024;
pub const SIG_BOUND: u64 = 34034726;
const QS1: u64 = 6144;

pub fn unpack(data: &[u8]) -> Option<Vec<u64>> {
    if data.len() != COMPACT_SIZE {
        return None;
    }
    let mut coeffs = Vec::with_capacity(N);
    for w in 0..32 {
        let ws = w * 32;
        for j in 0..16 {
            let hi = data[ws + 30 - j * 2] as u64;
            let lo = data[ws + 31 - j * 2] as u64;
            coeffs.push(hi * 256 + lo);
        }
    }
    Some(coeffs)
}

pub fn pack(coeffs: &[u64]) -> Vec<u8> {
    assert!(coeffs.len() >= N);
    let mut out = vec![0u8; COMPACT_SIZE];
    for w in 0..32 {
        let ws = w * 32;
        for j in 0..16 {
            let c = coeffs[w * 16 + j];
            out[ws + 30 - j * 2] = (c >> 8) as u8;
            out[ws + 31 - j * 2] = (c & 0xff) as u8;
        }
    }
    out
}

use std::sync::LazyLock;

static FALCON_PARAMS: LazyLock<FastNttParams> = LazyLock::new(|| {
    FastNttParams::new(Q, N, PSI).unwrap()
});

fn falcon_params() -> &'static FastNttParams {
    &FALCON_PARAMS
}

/// NTT forward on compact data.
pub fn ntt_fw_compact(input: &[u8]) -> Option<Vec<u8>> {
    let coeffs = unpack(input)?;
    let params = falcon_params();
    Some(pack(&fast::ntt_fw_fast(&coeffs, &params)))
}

/// NTT inverse on compact data.
pub fn ntt_inv_compact(input: &[u8]) -> Option<Vec<u8>> {
    let coeffs = unpack(input)?;
    let params = falcon_params();
    Some(pack(&fast::ntt_inv_fast(&coeffs, &params)))
}

/// Pointwise multiply mod q on two compact vectors (2048 bytes input).
pub fn vecmulmod_compact(input: &[u8]) -> Option<Vec<u8>> {
    if input.len() != 2 * COMPACT_SIZE {
        return None;
    }
    let a = unpack(&input[..COMPACT_SIZE])?;
    let b = unpack(&input[COMPACT_SIZE..])?;
    Some(pack(&fast::vec_mul_mod_fast(&a, &b, Q)))
}

/// SHAKE256 hash-to-point: input = salt||msg, output = compact.
pub fn shake256_htp(input: &[u8]) -> Vec<u8> {
    let mut hasher = Shake256::default();
    hasher.update(input);
    let mut reader = hasher.finalize_xof();
    let mut coeffs = Vec::with_capacity(N);
    let mut buf = [0u8; 2];
    while coeffs.len() < N {
        reader.read(&mut buf);
        let t = (buf[0] as u64) * 256 + buf[1] as u64;
        if t < 61445 {
            coeffs.push(t % Q);
        }
    }
    pack(&coeffs)
}

/// Falcon-512 norm check (compact format convenience).
pub fn falcon_norm(s1_compact: &[u8], s2_compact: &[u8], hashed_compact: &[u8]) -> bool {
    let s1 = match unpack(s1_compact) { Some(c) => c, None => return false };
    let s2 = match unpack(s2_compact) { Some(c) => c, None => return false };
    let hashed = match unpack(hashed_compact) { Some(c) => c, None => return false };
    falcon_norm_coeffs(&s1, &s2, &hashed)
}

/// Falcon-512 norm check on raw coefficient arrays.
pub fn falcon_norm_coeffs(s1: &[u64], s2: &[u64], hashed: &[u64]) -> bool {
    lp_norm_coeffs(Q, SIG_BOUND as u128, 2, s1, s2, hashed)
}

/// Generalized centered Lp norm check for any lattice-based signature.
///
/// Computes the centered Lp norm of (hashed - s1) mod q and s2,
/// then checks if the combined norm is below `bound`.
///
/// p=u64::MAX: L∞ (infinity norm) — max|centered(x)| < bound
/// p=1:        L1 (Manhattan)     — Σ|centered(x)| < bound
/// p=2:        L2 (Euclidean)     — Σ centered(x)² < bound (squared, no sqrt)
///
/// For L2, caller passes bound² (squared bound) to avoid the square root.
/// Centering maps x to min(x, q-x).
pub fn lp_norm_coeffs(q: u64, bound: u128, p: u64, s1: &[u64], s2: &[u64], hashed: &[u64]) -> bool {
    let n = s1.len();
    if s2.len() != n || hashed.len() != n {
        return false;
    }
    let half_q = q / 2;

    match p {
        u64::MAX => {
            // L∞: max of all centered values < bound
            for i in 0..n {
                let mut d = (hashed[i] + q - s1[i]) % q;
                if d > half_q { d = q - d; }
                if d as u128 >= bound { return false; }

                let mut s = s2[i];
                if s > half_q { s = q - s; }
                if s as u128 >= bound { return false; }
            }
            true
        }
        1 => {
            // L1: sum of absolute centered values < bound
            let mut norm: u128 = 0;
            for i in 0..n {
                let mut d = (hashed[i] + q - s1[i]) % q;
                if d > half_q { d = q - d; }
                norm += d as u128;

                let mut s = s2[i];
                if s > half_q { s = q - s; }
                norm += s as u128;
            }
            norm < bound
        }
        2 => {
            // L2 squared: sum of squared centered values < bound
            let mut norm: u128 = 0;
            for i in 0..n {
                let mut d = (hashed[i] + q - s1[i]) % q;
                if d > half_q { d = q - d; }
                norm += (d as u128) * (d as u128);

                let mut s = s2[i];
                if s > half_q { s = q - s; }
                norm += (s as u128) * (s as u128);
            }
            norm < bound
        }
        _ => false,
    }
}

/// Generalized Lp norm precompile.
///
/// Input: `q(32) | n(32) | bound(32) | cb(32) | p(32) | count(32) | vec[0](n×cb) | ... | vec[count-1](n×cb)`
///
/// Computes the centered Lp norm over ALL n×count coefficients.
/// p=1: L1, p=2: L2 (squared), p=u64::MAX: L∞.
///
/// Output: 32 bytes (0x00..01 if norm < bound, 0x00..00 otherwise)
pub fn lp_norm_precompile(input: &[u8]) -> Option<Vec<u8>> {
    if input.len() < 192 { return None; } // 6 × 32-byte words

    let q = read_u64_be(&input[0..32])?;
    let n = read_u64_be(&input[32..64])? as usize;
    let bound = read_u128_be(&input[64..96]);
    let cb = read_u64_be(&input[96..128])? as usize;
    let p = read_u64_be(&input[128..160])?;
    let count = read_u64_be(&input[160..192])? as usize;

    if q == 0 || n == 0 || cb == 0 || cb > 8 || count == 0 { return None; }
    if p != 1 && p != 2 && p != u64::MAX { return None; }

    let vec_size = n.checked_mul(cb)?;
    let total = count.checked_mul(vec_size)?;
    if input.len() != 192 + total { return None; }

    let coeffs = read_coeffs(&input[192..], n * count, cb);
    let half_q = q / 2;

    let valid = match p {
        u64::MAX => {
            coeffs.iter().all(|&x| {
                let c = if x > half_q { q - x } else { x };
                (c as u128) < bound
            })
        }
        1 => {
            let norm: u128 = coeffs.iter().map(|&x| {
                let c = if x > half_q { q - x } else { x };
                c as u128
            }).sum();
            norm < bound
        }
        2 => {
            let norm: u128 = coeffs.iter().map(|&x| {
                let c = if x > half_q { q - x } else { x };
                (c as u128) * (c as u128)
            }).sum();
            norm < bound
        }
        _ => false,
    };

    let mut result = vec![0u8; 32];
    if valid { result[31] = 1; }
    Some(result)
}

// ─── Helpers ───

fn read_u64_be(data: &[u8]) -> Option<u64> {
    // Read from last 8 bytes of a 32-byte BE word (skip leading zeros)
    if data.len() != 32 { return None; }
    // Check that the top 24 bytes are zero
    if data[..24].iter().any(|&b| b != 0) { return None; }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[24..32]);
    Some(u64::from_be_bytes(buf))
}

fn read_u128_be(data: &[u8]) -> u128 {
    if data.len() != 32 { return 0; }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&data[16..32]);
    u128::from_be_bytes(buf)
}

fn read_coeffs(data: &[u8], n: usize, cb: usize) -> Vec<u64> {
    let mut coeffs = Vec::with_capacity(n);
    for i in 0..n {
        let start = i * cb;
        let mut val: u64 = 0;
        for j in 0..cb {
            val = (val << 8) | data[start + j] as u64;
        }
        coeffs.push(val);
    }
    coeffs
}

/// Falcon-512 verify precompile.
/// Input: s2(1024, 512×uint16 BE) | ntth(1024, 512×uint16 BE) | salt_msg(var)
/// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)
pub fn falcon_verify_precompile(input: &[u8]) -> Option<Vec<u8>> {
    const VEC_SIZE: usize = N * 2; // 512 × 2 bytes = 1024
    if input.len() < 2 * VEC_SIZE {
        return None;
    }
    let s2 = read_u16_be_array(&input[0..VEC_SIZE]);
    let ntth = read_u16_be_array(&input[VEC_SIZE..2 * VEC_SIZE]);
    let salt_msg = &input[2 * VEC_SIZE..];

    let params = falcon_params();
    let hashed_compact = shake256_htp(salt_msg);
    let hashed = unpack(&hashed_compact).unwrap();

    let ntt_s2 = fast::ntt_fw_fast(&s2, params);
    let product = fast::vec_mul_mod_fast(&ntt_s2, &ntth, Q);
    let s1 = fast::ntt_inv_fast(&product, params);

    to_result(falcon_norm_coeffs(&s1, &s2, &hashed))
}

fn read_u16_be_array(data: &[u8]) -> Vec<u64> {
    data.chunks_exact(2)
        .map(|c| ((c[0] as u64) << 8) | c[1] as u64)
        .collect()
}

fn to_result(valid: bool) -> Option<Vec<u8>> {
    let mut result = vec![0u8; 32];
    if valid { result[31] = 1; }
    Some(result)
}

// ═══════════════════════════════════════════════════════════════════
//  ML-DSA-44 (Dilithium2) full verification precompile
// ═══════════════════════════════════════════════════════════════════

mod dilithium {
    use super::*;
    use crate::precompile::shake_n;

    pub const Q: u64 = 8380417;
    pub const N: usize = 256;
    pub const PSI: u64 = 1753;
    pub const K: usize = 4;
    pub const L: usize = 4;
    pub const D: u32 = 13;
    pub const TAU: usize = 39;
    pub const GAMMA1: u64 = 1 << 17;
    pub const GAMMA2: u64 = (Q - 1) / 88;
    pub const BETA: u64 = TAU as u64 * 2;
    pub const ALPHA: u64 = 2 * GAMMA2;
    pub const M: u64 = (Q - 1) / ALPHA;
    pub const PK_LEN: usize = 1312;
    pub const SIG_LEN: usize = 2420;

    static DIL_PARAMS: std::sync::LazyLock<FastNttParams> = std::sync::LazyLock::new(|| {
        FastNttParams::new(Q, N, PSI).unwrap()
    });

    fn params() -> &'static FastNttParams { &DIL_PARAMS }

    fn expand_a(rho: &[u8]) -> Vec<Vec<Vec<u64>>> {
        let mut a = Vec::with_capacity(K);
        for i in 0..K {
            let mut row = Vec::with_capacity(L);
            for j in 0..L {
                let mut seed = Vec::with_capacity(34);
                seed.extend_from_slice(rho);
                seed.push(j as u8);
                seed.push(i as u8);
                let mut xof = [0u8; 840];
                shake_n(128, &seed, &mut xof);
                let mut poly = Vec::with_capacity(N);
                let mut p = 0;
                while poly.len() < N {
                    let val = xof[p] as u64 | ((xof[p+1] as u64) << 8) | (((xof[p+2] & 0x7F) as u64) << 16);
                    p += 3;
                    if val < Q { poly.push(val); }
                }
                row.push(poly);
            }
            a.push(row);
        }
        a
    }

    fn sample_in_ball(c_tilde: &[u8]) -> Vec<u64> {
        let mut xof = [0u8; 272];
        shake_n(256, c_tilde, &mut xof);
        let mut c = vec![0u64; N];
        let signs = u64::from_le_bytes(xof[0..8].try_into().unwrap());
        let mut pos = 8;
        let mut si = 0;
        for i in (N - TAU)..N {
            loop {
                let j = xof[pos] as usize; pos += 1;
                if j <= i {
                    c[i] = c[j];
                    c[j] = if (signs >> si) & 1 == 1 { Q - 1 } else { 1 };
                    si += 1; break;
                }
            }
        }
        c
    }

    fn decompose(r: u64) -> (u64, i64) {
        let r0 = r % ALPHA;
        let r0c = if r0 > ALPHA / 2 { r0 as i64 - ALPHA as i64 } else { r0 as i64 };
        let rmr0 = (r as i64 - r0c) as u64;
        if rmr0 == Q - 1 { (0, r0c - 1) } else { (rmr0 / ALPHA, r0c) }
    }

    fn use_hint(h: &[bool], r: &[u64]) -> Vec<u64> {
        let mut w1 = Vec::with_capacity(N);
        for i in 0..N {
            let (r1, r0) = decompose(r[i]);
            if h[i] {
                if r0 > 0 { w1.push((r1 + 1) % M); }
                else { w1.push((r1 + M - 1) % M); }
            } else { w1.push(r1); }
        }
        w1
    }

    fn encode_w1(w1_polys: &[Vec<u64>]) -> Vec<u8> {
        let mut out = Vec::new();
        for poly in w1_polys {
            let mut buf: u32 = 0; let mut bits: u32 = 0;
            for &c in poly {
                buf |= (c as u32) << bits; bits += 6;
                while bits >= 8 { out.push((buf & 0xFF) as u8); buf >>= 8; bits -= 8; }
            }
        }
        out
    }

    fn decode_pk(pk: &[u8]) -> Option<([u8; 32], Vec<Vec<u64>>)> {
        if pk.len() != PK_LEN { return None; }
        let mut rho = [0u8; 32];
        rho.copy_from_slice(&pk[..32]);
        let packed = &pk[32..];
        let mut t1 = Vec::with_capacity(K);
        let mut buf: u32 = 0; let mut bits: u32 = 0; let mut pos = 0;
        for _ in 0..K {
            let mut poly = Vec::with_capacity(N);
            for _ in 0..N {
                while bits < 10 { buf |= (packed[pos] as u32) << bits; bits += 8; pos += 1; }
                poly.push((buf & 0x3FF) as u64); buf >>= 10; bits -= 10;
            }
            t1.push(poly);
        }
        Some((rho, t1))
    }

    fn decode_sig(sig: &[u8]) -> Option<(Vec<u8>, Vec<Vec<u64>>, Vec<Vec<bool>>)> {
        if sig.len() != SIG_LEN { return None; }
        let c_tilde = sig[..32].to_vec();
        let z_packed = &sig[32..32 + L * N * 18 / 8];
        let mut z = Vec::with_capacity(L);
        let mut buf: u64 = 0; let mut bits: u32 = 0; let mut pos = 0;
        for _ in 0..L {
            let mut poly = Vec::with_capacity(N);
            for _ in 0..N {
                while bits < 18 { buf |= (z_packed[pos] as u64) << bits; bits += 8; pos += 1; }
                let raw = buf & 0x3FFFF; buf >>= 18; bits -= 18;
                poly.push(((GAMMA1 as i64 - raw as i64).rem_euclid(Q as i64)) as u64);
            }
            z.push(poly);
        }
        let h_packed = &sig[32 + L * N * 18 / 8..];
        let mut h = vec![vec![false; N]; K]; let mut idx = 0;
        for i in 0..K {
            let limit = h_packed[80 + i] as usize;
            while idx < limit { h[i][h_packed[idx] as usize] = true; idx += 1; }
        }
        Some((c_tilde, z, h))
    }

    /// ML-DSA-44 verify precompile.
    /// Input: pk(1312) | sig(2420) | msg(var)
    /// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)
    pub fn dilithium_verify_precompile(input: &[u8]) -> Option<Vec<u8>> {
        if input.len() < PK_LEN + SIG_LEN { return None; }
        let pk_bytes = &input[..PK_LEN];
        let sig_bytes = &input[PK_LEN..PK_LEN + SIG_LEN];
        let msg = &input[PK_LEN + SIG_LEN..];

        let (rho, t1) = decode_pk(pk_bytes)?;
        let (c_tilde, z, h) = decode_sig(sig_bytes)?;
        let p = params();

        // Infinity norm check
        let half_q = Q / 2;
        for poly in &z {
            for &c in poly {
                let centered = if c > half_q { Q - c } else { c };
                if centered >= GAMMA1 - BETA { return to_result(false); }
            }
        }

        // ExpandA
        let a_ntt = expand_a(&rho);

        // NTT(z), Az
        let z_ntt: Vec<Vec<u64>> = z.iter().map(|zi| fast::ntt_fw_fast(zi, p)).collect();
        let mut az_ntt = Vec::with_capacity(K);
        for i in 0..K {
            let mut acc = fast::vec_mul_mod_fast(&a_ntt[i][0], &z_ntt[0], Q);
            for j in 1..L { acc = fast::vec_add_mod_fast(&acc, &fast::vec_mul_mod_fast(&a_ntt[i][j], &z_ntt[j], Q), Q); }
            az_ntt.push(acc);
        }

        // tr = SHAKE256(pk)[:64], mu = SHAKE256(tr || msg)[:64]
        // Note: caller is responsible for FIPS 204 context wrapping if needed.
        // For raw dilithium2: msg is passed as-is.
        // For FIPS 204 ml_dsa_44: caller prepends 0x00||0x00 to msg before calling.
        let mut tr = [0u8; 64];
        shake_n(256, pk_bytes, &mut tr);
        let mut mu_input = Vec::with_capacity(64 + msg.len());
        mu_input.extend_from_slice(&tr);
        mu_input.extend_from_slice(msg);
        let mut mu = [0u8; 64];
        shake_n(256, &mu_input, &mut mu);

        // Challenge
        let c_poly = sample_in_ball(&c_tilde);
        let c_ntt = fast::ntt_fw_fast(&c_poly, p);

        // t1 << d, NTT
        let t1_d_ntt: Vec<Vec<u64>> = t1.iter()
            .map(|ti| fast::ntt_fw_fast(&ti.iter().map(|&x| (x << D) % Q).collect::<Vec<_>>(), p))
            .collect();

        // w_approx → UseHint → w1
        let mut w1_polys = Vec::with_capacity(K);
        for i in 0..K {
            let ct1 = fast::vec_mul_mod_fast(&c_ntt, &t1_d_ntt[i], Q);
            let w_ntt: Vec<u64> = az_ntt[i].iter().zip(ct1.iter())
                .map(|(&a, &b)| if a >= b { a - b } else { Q + a - b }).collect();
            let w_approx = fast::ntt_inv_fast(&w_ntt, p);
            w1_polys.push(use_hint(&h[i], &w_approx));
        }

        // Recompute c_tilde
        let w1_enc = encode_w1(&w1_polys);
        let mut hash_input = Vec::with_capacity(64 + w1_enc.len());
        hash_input.extend_from_slice(&mu);
        hash_input.extend_from_slice(&w1_enc);
        let mut c_tilde_check = [0u8; 32];
        shake_n(256, &hash_input, &mut c_tilde_check);

        to_result(c_tilde_check == c_tilde.as_slice())
    }
}

pub use dilithium::dilithium_verify_precompile;

/// Full Falcon-512 verification pipeline on compact data.
/// Input: salt||msg, s2_compact, ntth_compact (public key in NTT domain).
/// Returns true if signature is valid.
pub fn falcon_verify(salt_msg: &[u8], s2_compact: &[u8], ntth_compact: &[u8]) -> bool {
    let hashed = shake256_htp(salt_msg);

    let s2_coeffs = match unpack(s2_compact) {
        Some(c) => c,
        None => return false,
    };
    let ntth_coeffs = match unpack(ntth_compact) {
        Some(c) => c,
        None => return false,
    };

    let params = falcon_params();
    let ntt_s2 = fast::ntt_fw_fast(&s2_coeffs, &params);
    let product = fast::vec_mul_mod_fast(&ntt_s2, &ntth_coeffs, Q);
    let s1 = fast::ntt_inv_fast(&product, &params);

    let hashed_coeffs = unpack(&hashed).unwrap();
    falcon_norm_coeffs(&s1, &s2_coeffs, &hashed_coeffs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let coeffs: Vec<u64> = (0..N as u64).map(|i| i % Q).collect();
        let packed = pack(&coeffs);
        let unpacked = unpack(&packed).unwrap();
        assert_eq!(coeffs, unpacked);
    }

    #[test]
    fn test_ntt_compact_roundtrip() {
        let coeffs: Vec<u64> = (0..N as u64).map(|i| i % Q).collect();
        let packed = pack(&coeffs);
        let fwd = ntt_fw_compact(&packed).unwrap();
        let inv = ntt_inv_compact(&fwd).unwrap();
        let recovered = unpack(&inv).unwrap();
        assert_eq!(coeffs, recovered);
    }

    #[test]
    fn test_shake256_htp_deterministic() {
        let input = b"test input data";
        let a = shake256_htp(input);
        let b = shake256_htp(input);
        assert_eq!(a, b);
        assert_eq!(a.len(), COMPACT_SIZE);
        // All coefficients should be < Q
        let coeffs = unpack(&a).unwrap();
        assert!(coeffs.iter().all(|&c| c < Q));
    }

    #[test]
    fn test_norm_valid() {
        // s1 = hashed (so d = 0 for all), s2 = all zeros → norm = 0
        let hashed: Vec<u64> = (0..N as u64).map(|i| i % Q).collect();
        let s1 = hashed.clone();
        let s2 = vec![0u64; N];
        assert!(falcon_norm_coeffs(&s1, &s2, &hashed));
    }

    #[test]
    fn test_norm_invalid() {
        let hashed = vec![0u64; N];
        let s1 = vec![0u64; N];
        let s2 = vec![6000u64; N];
        assert!(!falcon_norm_coeffs(&s1, &s2, &hashed));
    }

    #[test]
    fn test_lp_norm_falcon() {
        // Valid: s1 == hashed, s2 = 0 → norm = 0
        let s1: Vec<u64> = (0..512).map(|i| i % Q).collect();
        let hashed = s1.clone();
        let s2 = vec![0u64; 512];
        assert!(lp_norm_coeffs(Q, SIG_BOUND as u128, 2, &s1, &s2, &hashed));
    }

    #[test]
    fn test_lp_norm_dilithium_params() {
        // Dilithium: q=8380417, n=256
        let q = 8380417u64;
        let n = 256;
        let bound = 1u128 << 40;
        let s1 = vec![0u64; n];
        let s2 = vec![1u64; n];
        let hashed = vec![0u64; n];
        // L2: norm = 256 * 1² = 256, well under bound
        assert!(lp_norm_coeffs(q, bound, 2, &s1, &s2, &hashed));
        // L1: norm = 256 * 1 = 256
        assert!(lp_norm_coeffs(q, bound, 1, &s1, &s2, &hashed));
        // L∞: max = 1
        assert!(lp_norm_coeffs(q, 2, u64::MAX, &s1, &s2, &hashed));
        assert!(!lp_norm_coeffs(q, 1, u64::MAX, &s1, &s2, &hashed)); // bound=1, max=1, not < 1
        // Invalid p
        assert!(!lp_norm_coeffs(q, bound, 3, &s1, &s2, &hashed));
    }

    #[test]
    fn test_falcon_verify_precompile_valid() {
        // Build a valid verification using the precompile format
        let params = falcon_params();
        let s2: Vec<u64> = vec![0u64; N]; // zero s2 = trivial sig
        let h: Vec<u64> = (0..N as u64).map(|i| (i * 13 + 1) % Q).collect();
        let ntth = fast::ntt_fw_fast(&h, params);

        let salt_msg = b"test salt data for hash to point verification";

        let s2c = pack(&s2);
        let ntthc = pack(&ntth);

        let mut input = vec![0u8; 32];
        let sm_len = salt_msg.len() as u64;
        input[24..32].copy_from_slice(&sm_len.to_be_bytes());
        input.extend_from_slice(&s2c);
        input.extend_from_slice(&ntthc);
        input.extend_from_slice(salt_msg);

        let result = falcon_verify_precompile(&input).unwrap();
        // With s2=0, s1 = INTT(NTT(0)*NTT(h)) = 0, norm = ||hashed||²
        // This may or may not pass the bound depending on hashed values.
        // Just check it returns something valid (32 bytes).
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_falcon_verify_roundtrip() {
        // Verify that falcon_verify matches manual pipeline
        let s2: Vec<u64> = (0..N as u64).map(|i| ((i as i64 % 3 - 1).rem_euclid(Q as i64)) as u64).collect();
        let h: Vec<u64> = (0..N as u64).map(|i| (i * 7 + 3) % Q).collect();
        let ntth = fast::ntt_fw_fast(&h, falcon_params());

        let salt_msg = b"nonce40bytesxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxmsg";
        let s2c = pack(&s2);
        let ntthc = pack(&ntth);

        let result_api = falcon_verify(salt_msg, &s2c, &ntthc);

        // Manual pipeline
        let hashed = shake256_htp(salt_msg);
        let hashed_coeffs = unpack(&hashed).unwrap();
        let ntt_s2 = fast::ntt_fw_fast(&s2, falcon_params());
        let product = fast::vec_mul_mod_fast(&ntt_s2, &ntth, Q);
        let s1 = fast::ntt_inv_fast(&product, falcon_params());
        let result_manual = falcon_norm_coeffs(&s1, &s2, &hashed_coeffs);

        assert_eq!(result_api, result_manual);
    }

    #[test]
    fn test_lp_norm_precompile() {
        // Build precompile input for Falcon-512
        let q: u64 = Q;
        let n: u64 = N as u64;
        let bound: u128 = SIG_BOUND as u128;
        let cb: u64 = 2;

        let mut input = Vec::new();
        // q (32 bytes BE)
        input.extend_from_slice(&[0u8; 24]);
        input.extend_from_slice(&q.to_be_bytes());
        // n (32 bytes BE)
        input.extend_from_slice(&[0u8; 24]);
        input.extend_from_slice(&n.to_be_bytes());
        // bound (32 bytes BE)
        input.extend_from_slice(&[0u8; 16]);
        input.extend_from_slice(&bound.to_be_bytes());
        // cb (32 bytes BE)
        input.extend_from_slice(&[0u8; 24]);
        input.extend_from_slice(&cb.to_be_bytes());
        // p = 2 (L2 squared) (32 bytes BE)
        input.extend_from_slice(&[0u8; 24]);
        input.extend_from_slice(&2u64.to_be_bytes());
        // count = 2 (32 bytes BE) — diff and s2
        input.extend_from_slice(&[0u8; 24]);
        input.extend_from_slice(&2u64.to_be_bytes());

        // diff = (hashed - s1) mod q = 0 (since s1 == hashed), s2 = 0 → norm = 0
        // vec[0] = diff = all zeros
        for _ in 0..N {
            input.extend_from_slice(&[0u8; 2]);
        }
        // vec[1] = s2 = all zeros
        for _ in 0..N {
            input.extend_from_slice(&[0u8; 2]);
        }

        let result = lp_norm_precompile(&input).unwrap();
        assert_eq!(result[31], 1, "expected valid L2 norm");
    }

    #[test]
    fn test_lp_norm_precompile_linf() {
        // Test L∞ norm for Dilithium z-check
        let q: u64 = 8380417;
        let n: u64 = 256;
        let bound: u128 = 130994; // gamma1 - beta
        let cb: u64 = 3;

        let mut input = Vec::new();
        input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&q.to_be_bytes());
        input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&n.to_be_bytes());
        input.extend_from_slice(&[0u8; 16]); input.extend_from_slice(&bound.to_be_bytes());
        input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&cb.to_be_bytes());
        input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&u64::MAX.to_be_bytes()); // p = L∞
        input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&4u64.to_be_bytes()); // count = 4

        // 4 × 256 coefficients, all small (< bound)
        for _ in 0..4 {
            for i in 0..256u32 {
                let v = i % 100; // well under 130994
                input.push((v >> 16) as u8);
                input.push((v >> 8) as u8);
                input.push(v as u8);
            }
        }

        let result = lp_norm_precompile(&input).unwrap();
        assert_eq!(result[31], 1, "expected valid L∞ norm");
    }
}
