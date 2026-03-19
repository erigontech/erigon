use crate::falcon;
use crate::fast::{self, FastNttParams};
use crate::precompile::PrecompileError;
use std::slice;

fn error_code(e: PrecompileError) -> i32 {
    match e {
        PrecompileError::InputTooShort => -1,
        PrecompileError::InvalidParams(_) => -2,
        PrecompileError::BadLength => -3,
        PrecompileError::Overflow(_) => -4,
    }
}

fn write_output(output: Vec<u8>, out_ptr: *mut *mut u8, out_len: *mut usize) {
    let boxed = output.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut u8;
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
}

// ─── Precompile entry points ───

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fw_precompile(
    input: *const u8,
    input_len: usize,
    output_out: *mut *mut u8,
    output_len_out: *mut usize,
) -> i32 {
    let input = slice::from_raw_parts(input, input_len);
    match crate::ntt_fw_precompile(input) {
        Ok(output) => {
            write_output(output, output_out, output_len_out);
            0
        }
        Err(e) => error_code(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_inv_precompile(
    input: *const u8,
    input_len: usize,
    output_out: *mut *mut u8,
    output_len_out: *mut usize,
) -> i32 {
    let input = slice::from_raw_parts(input, input_len);
    match crate::ntt_inv_precompile(input) {
        Ok(output) => {
            write_output(output, output_out, output_len_out);
            0
        }
        Err(e) => error_code(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_vecmulmod_precompile(
    input: *const u8,
    input_len: usize,
    output_out: *mut *mut u8,
    output_len_out: *mut usize,
) -> i32 {
    let input = slice::from_raw_parts(input, input_len);
    match crate::ntt_vecmulmod_precompile(input) {
        Ok(output) => {
            write_output(output, output_out, output_len_out);
            0
        }
        Err(e) => error_code(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_vecaddmod_precompile(
    input: *const u8,
    input_len: usize,
    output_out: *mut *mut u8,
    output_len_out: *mut usize,
) -> i32 {
    let input = slice::from_raw_parts(input, input_len);
    match crate::ntt_vecaddmod_precompile(input) {
        Ok(output) => {
            write_output(output, output_out, output_len_out);
            0
        }
        Err(e) => error_code(e),
    }
}

/// Element-wise (a[i] - b[i]) mod q. Same format as VECADDMOD.
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_vecsubmod_precompile(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let input = slice::from_raw_parts(input, input_len);
    match crate::ntt_vecsubmod_precompile(input) {
        Ok(output) => { write_output(output, output_out, output_len_out); 0 }
        Err(e) => error_code(e),
    }
}

/// ExpandA + matrix-vector multiply.
/// Input: q(32) | n(32) | k(32) | l(32) | rho(32) | z(l*n*cb)
/// Output: k*n*cb bytes
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_expand_a_vecmul_precompile(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let input = slice::from_raw_parts(input, input_len);
    match crate::expand_a_vecmul_precompile(input) {
        Ok(output) => { write_output(output, output_out, output_len_out); 0 }
        Err(e) => error_code(e),
    }
}

/// Falcon-512 verify.
/// Input: s2(1024, 512×uint16 BE) | ntth(1024, 512×uint16 BE) | salt_msg(var)
/// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_falcon_verify(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let data = slice::from_raw_parts(input, input_len);
    match falcon::falcon_verify_precompile(data) {
        Some(out) => { write_output(out, output_out, output_len_out); 0 }
        None => -1,
    }
}

/// Generic SHAKE-N (SHAKE128 / SHAKE256).
/// Input: security(32 BE) | output_len(32 BE) | data(var)
/// Output: output_len bytes of SHAKE output.
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_shake(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let data = slice::from_raw_parts(input, input_len);
    match crate::shake_precompile(data) {
        Ok(out) => { write_output(out, output_out, output_len_out); 0 }
        Err(e) => error_code(e),
    }
}

/// SHAKE256 Hash-to-Point with rejection sampling.
/// Input: output_len(32 BE) | data(var)
/// Returns output_len bytes of rejection-sampled coefficients mod Q=12289, packed as uint16 BE.
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_shake256_htp(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let data = slice::from_raw_parts(input, input_len);
    match crate::shake256_htp_precompile(data) {
        Ok(out) => { write_output(out, output_out, output_len_out); 0 }
        Err(e) => error_code(e),
    }
}

/// ML-DSA-44 (Dilithium2) full verification.
/// Input: pk(1312) | sig(2420) | msg(var)
/// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_dilithium_verify(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let data = slice::from_raw_parts(input, input_len);
    match falcon::dilithium_verify_precompile(data) {
        Some(out) => { write_output(out, output_out, output_len_out); 0 }
        None => -1,
    }
}

/// Generalized LpNorm for any lattice-based signature.
/// Input: q(32) | n(32) | bound(32) | cb(32) | s1(n×cb) | s2(n×cb) | hashed(n×cb)
#[no_mangle]
pub unsafe extern "C" fn eth_ntt_lp_norm(
    input: *const u8, input_len: usize,
    output_out: *mut *mut u8, output_len_out: *mut usize,
) -> i32 {
    let data = slice::from_raw_parts(input, input_len);
    match falcon::lp_norm_precompile(data) {
        Some(out) => { write_output(out, output_out, output_len_out); 0 }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_free_buffer(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(ptr, len));
    }
}

// ─── Fast direct API ───

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fast_params_new(
    q: u64,
    n: usize,
    psi: u64,
) -> *mut FastNttParams {
    match FastNttParams::new(q, n, psi) {
        Ok(params) => Box::into_raw(Box::new(params)),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fast_params_free(params: *mut FastNttParams) {
    if !params.is_null() {
        drop(Box::from_raw(params));
    }
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fast_params_q(params: *const FastNttParams) -> u64 {
    (*params).q
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fast_params_n(params: *const FastNttParams) -> usize {
    (*params).n
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fast_params_coeff_bytes(params: *const FastNttParams) -> usize {
    (*params).coeff_bytes
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_fw(
    params: *const FastNttParams,
    input: *const u64,
    output: *mut u64,
    n: usize,
) {
    let params = &*params;
    let input = slice::from_raw_parts(input, n);
    let result = fast::ntt_fw_fast(input, params);
    std::ptr::copy_nonoverlapping(result.as_ptr(), output, n);
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_inv(
    params: *const FastNttParams,
    input: *const u64,
    output: *mut u64,
    n: usize,
) {
    let params = &*params;
    let input = slice::from_raw_parts(input, n);
    let result = fast::ntt_inv_fast(input, params);
    std::ptr::copy_nonoverlapping(result.as_ptr(), output, n);
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_vec_mul_mod(
    a: *const u64,
    b: *const u64,
    output: *mut u64,
    n: usize,
    q: u64,
) {
    let a = slice::from_raw_parts(a, n);
    let b = slice::from_raw_parts(b, n);
    let result = fast::vec_mul_mod_fast(a, b, q);
    std::ptr::copy_nonoverlapping(result.as_ptr(), output, n);
}

#[no_mangle]
pub unsafe extern "C" fn eth_ntt_vec_add_mod(
    a: *const u64,
    b: *const u64,
    output: *mut u64,
    n: usize,
    q: u64,
) {
    let a = slice::from_raw_parts(a, n);
    let b = slice::from_raw_parts(b, n);
    let result = fast::vec_add_mod_fast(a, b, q);
    std::ptr::copy_nonoverlapping(result.as_ptr(), output, n);
}
