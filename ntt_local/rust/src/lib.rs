pub mod falcon;
pub mod fast;
mod ffi;
mod field;
mod ntt;
mod precompile;

pub use fast::{FastNttParams, ntt_fw_fast, ntt_inv_fast, vec_add_mod_fast, vec_mul_mod_fast};
pub use field::FieldParams;
pub use ntt::{ntt_fw, ntt_inv, vec_add_mod, vec_mul_mod};
pub use precompile::{
    decode_output, encode_ntt_input, encode_vec_input, ntt_fw_precompile, ntt_inv_precompile,
    expand_a_vecmul_precompile, ntt_vecaddmod_precompile, ntt_vecmulmod_precompile,
    ntt_vecsubmod_precompile, shake256_htp_precompile, shake_n, shake_precompile,
    PrecompileError,
};
