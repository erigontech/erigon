use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pq_eth_precompiles::{
    decode_output, encode_ntt_input, encode_vec_input, ntt_fw, ntt_fw_fast, ntt_fw_precompile,
    ntt_inv, ntt_inv_fast, ntt_inv_precompile, ntt_vecaddmod_precompile,
    ntt_vecmulmod_precompile, vec_add_mod, vec_add_mod_fast, vec_mul_mod, vec_mul_mod_fast,
    FastNttParams, FieldParams,
};
use num_bigint::BigUint;
use num_traits::One;

// ─── Parameter sets ───

fn falcon512_params_big() -> FieldParams {
    let q = BigUint::from(12289u64);
    let g = BigUint::from(11u64);
    let exp = (&q - BigUint::one()) / BigUint::from(1024u64);
    let psi = g.modpow(&exp, &q);
    FieldParams::new(q, 512, psi).unwrap()
}

fn falcon512_params_fast() -> FastNttParams {
    let q = 12289u64;
    let psi = fast_pow_mod(11, (q - 1) / 1024, q);
    FastNttParams::new(q, 512, psi).unwrap()
}

fn dilithium_params_fast() -> FastNttParams {
    let q = 8380417u64;
    let n = 256usize;
    let psi = fast_pow_mod(7, (q - 1) / (2 * n as u64), q);
    FastNttParams::new(q, n, psi).unwrap()
}

fn fast_pow_mod(mut base: u64, mut exp: u64, q: u64) -> u64 {
    let mut result = 1u64;
    base %= q;
    while exp > 0 {
        if exp & 1 == 1 {
            result = ((result as u128 * base as u128) % q as u128) as u64;
        }
        exp >>= 1;
        if exp > 0 {
            base = ((base as u128 * base as u128) % q as u128) as u64;
        }
    }
    result
}

fn make_poly_big(params: &FieldParams) -> Vec<BigUint> {
    let mut seed: u64 = 0xDEADBEEF;
    let q_u64 = params.q.to_bytes_be().iter().fold(0u64, |a, &b| a << 8 | b as u64);
    (0..params.n)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            BigUint::from(seed % q_u64)
        })
        .collect()
}

fn make_poly_u64(n: usize, q: u64) -> Vec<u64> {
    let mut seed: u64 = 0xDEADBEEF;
    (0..n)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed % q
        })
        .collect()
}

// ─── NTT forward: BigUint vs fast ───

fn bench_ntt_fw(c: &mut Criterion) {
    let mut group = c.benchmark_group("ntt_fw");

    // BigUint
    let falcon_big = falcon512_params_big();
    let poly_big = make_poly_big(&falcon_big);
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| ntt_fw(black_box(&poly_big), black_box(&falcon_big)))
    });

    // Fast u64
    let falcon_fast = falcon512_params_fast();
    let poly_u64 = make_poly_u64(512, 12289);
    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| ntt_fw_fast(black_box(&poly_u64), black_box(&falcon_fast)))
    });

    let dil_fast = dilithium_params_fast();
    let poly_dil = make_poly_u64(256, 8380417);
    group.bench_function("fast_u64/dilithium_n256", |b| {
        b.iter(|| ntt_fw_fast(black_box(&poly_dil), black_box(&dil_fast)))
    });

    group.finish();
}

// ─── NTT inverse: BigUint vs fast ───

fn bench_ntt_inv(c: &mut Criterion) {
    let mut group = c.benchmark_group("ntt_inv");

    let falcon_big = falcon512_params_big();
    let poly_big = make_poly_big(&falcon_big);
    let ntt_big = ntt_fw(&poly_big, &falcon_big);
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| ntt_inv(black_box(&ntt_big), black_box(&falcon_big)))
    });

    let falcon_fast = falcon512_params_fast();
    let poly_u64 = make_poly_u64(512, 12289);
    let ntt_u64 = ntt_fw_fast(&poly_u64, &falcon_fast);
    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| ntt_inv_fast(black_box(&ntt_u64), black_box(&falcon_fast)))
    });

    group.finish();
}

// ─── NTT roundtrip ───

fn bench_ntt_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("ntt_roundtrip");

    let falcon_big = falcon512_params_big();
    let poly_big = make_poly_big(&falcon_big);
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| {
            let fwd = ntt_fw(black_box(&poly_big), &falcon_big);
            ntt_inv(black_box(&fwd), &falcon_big)
        })
    });

    let falcon_fast = falcon512_params_fast();
    let poly_u64 = make_poly_u64(512, 12289);
    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| {
            let fwd = ntt_fw_fast(black_box(&poly_u64), &falcon_fast);
            ntt_inv_fast(black_box(&fwd), &falcon_fast)
        })
    });

    group.finish();
}

// ─── Vector operations ───

fn bench_vec_mul_mod(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec_mul_mod");

    let falcon_big = falcon512_params_big();
    let a_big = make_poly_big(&falcon_big);
    let b_big = make_poly_big(&falcon_big);
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| vec_mul_mod(black_box(&a_big), black_box(&b_big), black_box(&falcon_big.q)))
    });

    let a_u64 = make_poly_u64(512, 12289);
    let b_u64 = make_poly_u64(512, 12289);
    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| vec_mul_mod_fast(black_box(&a_u64), black_box(&b_u64), black_box(12289)))
    });

    group.finish();
}

fn bench_vec_add_mod(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec_add_mod");

    let falcon_big = falcon512_params_big();
    let a_big = make_poly_big(&falcon_big);
    let b_big = make_poly_big(&falcon_big);
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| vec_add_mod(black_box(&a_big), black_box(&b_big), black_box(&falcon_big.q)))
    });

    let a_u64 = make_poly_u64(512, 12289);
    let b_u64 = make_poly_u64(512, 12289);
    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| vec_add_mod_fast(black_box(&a_u64), black_box(&b_u64), black_box(12289)))
    });

    group.finish();
}

// ─── Polynomial multiplication (full NTT pipeline) ───

fn bench_polymul(c: &mut Criterion) {
    let mut group = c.benchmark_group("polymul_ntt");

    let falcon_big = falcon512_params_big();
    let f_big = make_poly_big(&falcon_big);
    let g_big = make_poly_big(&falcon_big);
    let q_big = &falcon_big.q;
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| {
            let nf = ntt_fw(black_box(&f_big), &falcon_big);
            let ng = ntt_fw(black_box(&g_big), &falcon_big);
            let prod = vec_mul_mod(&nf, &ng, q_big);
            ntt_inv(&prod, &falcon_big)
        })
    });

    let falcon_fast = falcon512_params_fast();
    let f_u64 = make_poly_u64(512, 12289);
    let g_u64 = make_poly_u64(512, 12289);
    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| {
            let nf = ntt_fw_fast(black_box(&f_u64), &falcon_fast);
            let ng = ntt_fw_fast(black_box(&g_u64), &falcon_fast);
            let prod = vec_mul_mod_fast(&nf, &ng, 12289);
            ntt_inv_fast(&prod, &falcon_fast)
        })
    });

    group.finish();
}

// ─── Precompile entry points (implicitly use fast path now) ───

fn bench_precompile_ntt(c: &mut Criterion) {
    let mut group = c.benchmark_group("precompile_ntt");

    let falcon = falcon512_params_big();
    let poly = make_poly_big(&falcon);
    let input = encode_ntt_input(&falcon, &poly);
    let cb = falcon.coeff_byte_len();

    group.bench_function("ntt_fw/falcon512", |b| {
        b.iter(|| ntt_fw_precompile(black_box(&input)))
    });

    let fwd_out = ntt_fw_precompile(&input).unwrap();
    let ntt_poly = decode_output(&fwd_out, falcon.n, cb);
    let inv_input = encode_ntt_input(&falcon, &ntt_poly);
    group.bench_function("ntt_inv/falcon512", |b| {
        b.iter(|| ntt_inv_precompile(black_box(&inv_input)))
    });

    group.finish();
}

fn bench_precompile_vec(c: &mut Criterion) {
    let mut group = c.benchmark_group("precompile_vec");

    let falcon = falcon512_params_big();
    let a = make_poly_big(&falcon);
    let b = make_poly_big(&falcon);
    let q = &falcon.q;
    let n = falcon.n;

    let mul_input = encode_vec_input(q, n, &a, &b);
    let add_input = encode_vec_input(q, n, &a, &b);

    group.bench_function("vecmulmod/falcon512", |b| {
        b.iter(|| ntt_vecmulmod_precompile(black_box(&mul_input)))
    });

    group.bench_function("vecaddmod/falcon512", |b| {
        b.iter(|| ntt_vecaddmod_precompile(black_box(&add_input)))
    });

    group.finish();
}

// ─── Table precomputation ───

fn bench_table_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_gen");

    let falcon_big = falcon512_params_big();
    group.bench_function("biguint/falcon512", |b| {
        b.iter(|| falcon_big.psi_rev_table())
    });

    group.bench_function("fast_u64/falcon512", |b| {
        b.iter(|| {
            let q = 12289u64;
            let psi = fast_pow_mod(11, (q - 1) / 1024, q);
            FastNttParams::new(q, 512, psi).unwrap()
        })
    });

    group.finish();
}

// ─── Full FALCON verify simulation ───

fn bench_falcon_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("falcon_verify_sim");
    group.sample_size(20);

    // BigUint path
    let p = falcon512_params_big();
    let q_big = &p.q;
    let h_big = make_poly_big(&p);
    let s2_big: Vec<BigUint> = (0..p.n)
        .map(|i| {
            let v = ((i * 3 + 1) % 5) as i64 - 2;
            if v >= 0 { BigUint::from(v as u64) } else { &p.q - BigUint::from((-v) as u64) }
        })
        .collect();
    let s2h_big = {
        let ns2 = ntt_fw(&s2_big, &p);
        let nh = ntt_fw(&h_big, &p);
        ntt_inv(&vec_mul_mod(&ns2, &nh, q_big), &p)
    };
    let s1_big: Vec<BigUint> = (0..p.n).map(|i| BigUint::from((i % 3) as u64)).collect();
    let c_big: Vec<BigUint> = s1_big.iter().zip(s2h_big.iter()).map(|(a, b)| (a + b) % q_big).collect();

    group.bench_function("biguint", |bench| {
        bench.iter(|| {
            let ns2 = ntt_fw(black_box(&s2_big), &p);
            let nh = ntt_fw(black_box(&h_big), &p);
            let prod = vec_mul_mod(&ns2, &nh, q_big);
            let t = ntt_inv(&prod, &p);
            let _s1: Vec<BigUint> = c_big.iter().zip(t.iter()).map(|(ci, ti)| {
                if ci >= ti { (ci - ti) % q_big } else { q_big - ((ti - ci) % q_big) }
            }).collect();
        })
    });

    // Fast u64 path
    let fp = falcon512_params_fast();
    let q = fp.q;
    let h_u64 = make_poly_u64(512, q);
    let s2_u64: Vec<u64> = (0..512)
        .map(|i| {
            let v = ((i * 3 + 1) % 5) as i64 - 2;
            if v >= 0 { v as u64 } else { q - (-v) as u64 }
        })
        .collect();
    let s2h_u64 = {
        let ns2 = ntt_fw_fast(&s2_u64, &fp);
        let nh = ntt_fw_fast(&h_u64, &fp);
        ntt_inv_fast(&vec_mul_mod_fast(&ns2, &nh, q), &fp)
    };
    let s1_u64: Vec<u64> = (0..512).map(|i| (i % 3) as u64).collect();
    let c_u64: Vec<u64> = s1_u64.iter().zip(s2h_u64.iter()).map(|(&a, &b)| (a + b) % q).collect();

    group.bench_function("fast_u64", |bench| {
        bench.iter(|| {
            let ns2 = ntt_fw_fast(black_box(&s2_u64), &fp);
            let nh = ntt_fw_fast(black_box(&h_u64), &fp);
            let prod = vec_mul_mod_fast(&ns2, &nh, q);
            let t = ntt_inv_fast(&prod, &fp);
            let _s1: Vec<u64> = c_u64.iter().zip(t.iter()).map(|(&ci, &ti)| {
                if ci >= ti { ci - ti } else { q + ci - ti }
            }).collect();
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ntt_fw,
    bench_ntt_inv,
    bench_ntt_roundtrip,
    bench_vec_mul_mod,
    bench_vec_add_mod,
    bench_polymul,
    bench_precompile_ntt,
    bench_precompile_vec,
    bench_table_generation,
    bench_falcon_verify,
);
criterion_main!(benches, compact_benches);

// ─── Compact format benchmarks ───

use pq_eth_precompiles::falcon;

fn bench_compact_ntt_fw(c: &mut Criterion) {
    let coeffs: Vec<u64> = (0..512).map(|i| i % 12289).collect();
    let input = falcon::pack(&coeffs);
    c.bench_function("compact_ntt_fw", |b| {
        b.iter(|| falcon::ntt_fw_compact(black_box(&input)))
    });
}

fn bench_compact_vecmulmod(c: &mut Criterion) {
    let a: Vec<u64> = (0..512).map(|i| i % 12289).collect();
    let b: Vec<u64> = (0..512).map(|i| (i * 7) % 12289).collect();
    let mut input = falcon::pack(&a);
    input.extend_from_slice(&falcon::pack(&b));
    c.bench_function("compact_vecmulmod", |b_| {
        b_.iter(|| falcon::vecmulmod_compact(black_box(&input)))
    });
}

fn bench_compact_shake256_htp(c: &mut Criterion) {
    let input = b"test salt and message data for hash to point";
    c.bench_function("compact_shake256_htp", |b| {
        b.iter(|| falcon::shake256_htp(black_box(input)))
    });
}

fn bench_compact_norm(c: &mut Criterion) {
    let s1: Vec<u64> = (0..512).map(|i| i % 12289).collect();
    let s2: Vec<u64> = (0..512).map(|i| (i * 3) % 12289).collect();
    let hashed: Vec<u64> = (0..512).map(|i| (i * 7 + 100) % 12289).collect();
    let s1c = falcon::pack(&s1);
    let s2c = falcon::pack(&s2);
    let hc = falcon::pack(&hashed);
    c.bench_function("compact_falcon_norm", |b| {
        b.iter(|| falcon::falcon_norm(black_box(&s1c), black_box(&s2c), black_box(&hc)))
    });
}

fn bench_compact_full_verify(c: &mut Criterion) {
    // Build a valid verification scenario
    let params = FastNttParams::new(12289, 512, 49).unwrap();
    let h: Vec<u64> = (0..512).map(|i| (i * 13 + 1) % 12289).collect();
    let s2: Vec<u64> = (0..512).map(|i| ((i as i64 * 3 % 5 - 2).rem_euclid(12289)) as u64).collect();
    let ntth = ntt_fw_fast(&h, &params);

    let salt_msg = b"test salt 40 bytes xxxxxxxxxxxxxxxxxxxxxxxxxx msg";
    let hashed = falcon::shake256_htp(salt_msg);
    let hashed_coeffs = falcon::unpack(&hashed).unwrap();

    let ntt_s2 = ntt_fw_fast(&s2, &params);
    let product = vec_mul_mod_fast(&ntt_s2, &ntth, 12289);
    let s1 = ntt_inv_fast(&product, &params);

    let s1c = falcon::pack(&s1);
    let s2c = falcon::pack(&s2);
    let ntthc = falcon::pack(&ntth);

    c.bench_function("compact_full_pipeline", |b| {
        b.iter(|| {
            let hashed = falcon::shake256_htp(black_box(salt_msg));
            let s2_coeffs = falcon::unpack(black_box(&s2c)).unwrap();
            let ntth_coeffs = falcon::unpack(black_box(&ntthc)).unwrap();
            let ntt_s2 = ntt_fw_fast(&s2_coeffs, &params);
            let product = vec_mul_mod_fast(&ntt_s2, &ntth_coeffs, 12289);
            let s1 = ntt_inv_fast(&product, &params);
            let hashed_c = falcon::unpack(&hashed).unwrap();
            falcon::falcon_norm_coeffs(&s1, &s2_coeffs, &hashed_c)
        })
    });
}

criterion_group!(
    compact_benches,
    bench_compact_ntt_fw,
    bench_compact_vecmulmod,
    bench_compact_shake256_htp,
    bench_compact_norm,
    bench_compact_full_verify,
    bench_falcon_verify_precompile,
    bench_shake_precompile,
    bench_lp_norm_precompile,
    bench_dilithium_precompile_ntt,
);

fn bench_shake_precompile(c: &mut Criterion) {
    let mut group = c.benchmark_group("shake256_precompile");

    // SHAKE256: format is output_len(32) | data
    // 32 bytes input, 32 bytes output
    let input_32 = {
        let mut v = vec![0u8; 32];
        v[31] = 32; // output_len = 32
        v.extend_from_slice(&[0x42u8; 32]);
        v
    };
    group.bench_function("32B_in_32B_out", |b| {
        b.iter(|| pq_eth_precompiles::shake_precompile(black_box(&input_32)))
    });

    // 832 bytes input (Dilithium challenge hash size), 32 bytes output
    let input_big = {
        let mut v = vec![0u8; 32];
        v[31] = 32;
        v.extend_from_slice(&[0x42u8; 832]);
        v
    };
    group.bench_function("832B_in_32B_out", |b| {
        b.iter(|| pq_eth_precompiles::shake_precompile(black_box(&input_big)))
    });

    group.finish();
}

fn bench_lp_norm_precompile(c: &mut Criterion) {
    let mut group = c.benchmark_group("lp_norm_precompile");

    // Falcon-512: q=12289, n=512, cb=2
    let q: u64 = 12289;
    let n: u64 = 512;
    let bound: u128 = 34034726;
    let cb: u64 = 2;
    let mut input = Vec::new();
    input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&q.to_be_bytes());
    input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&n.to_be_bytes());
    input.extend_from_slice(&[0u8; 16]); input.extend_from_slice(&bound.to_be_bytes());
    input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&cb.to_be_bytes());
    for _ in 0..3 { // s1, s2, hashed
        for i in 0..512u16 {
            input.extend_from_slice(&(i % q as u16).to_be_bytes());
        }
    }
    group.bench_function("falcon512", |b| {
        b.iter(|| falcon::lp_norm_precompile(black_box(&input)))
    });

    // Dilithium: q=8380417, n=256, cb=3
    let q: u64 = 8380417;
    let n: u64 = 256;
    let bound: u128 = 1 << 40;
    let cb: u64 = 3;
    let mut input = Vec::new();
    input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&q.to_be_bytes());
    input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&n.to_be_bytes());
    input.extend_from_slice(&[0u8; 16]); input.extend_from_slice(&bound.to_be_bytes());
    input.extend_from_slice(&[0u8; 24]); input.extend_from_slice(&cb.to_be_bytes());
    for _ in 0..3 {
        for i in 0..256u32 {
            let v = i % q as u32;
            input.push((v >> 16) as u8);
            input.push((v >> 8) as u8);
            input.push(v as u8);
        }
    }
    group.bench_function("dilithium_n256", |b| {
        b.iter(|| falcon::lp_norm_precompile(black_box(&input)))
    });

    group.finish();
}

fn bench_dilithium_precompile_ntt(c: &mut Criterion) {
    let mut group = c.benchmark_group("precompile_ntt_dilithium");

    let q = 8380417u64;
    let n = 256usize;
    let psi = fast_pow_mod(7, (q - 1) / (2 * n as u64), q);
    let params = FieldParams::new(BigUint::from(q), n, BigUint::from(psi)).unwrap();
    let poly: Vec<BigUint> = (0..n).map(|i| BigUint::from((i as u64 * 37) % q)).collect();
    let input = encode_ntt_input(&params, &poly);

    group.bench_function("ntt_fw", |b| {
        b.iter(|| ntt_fw_precompile(black_box(&input)))
    });

    let fwd_out = ntt_fw_precompile(&input).unwrap();
    let ntt_poly = decode_output(&fwd_out, n, 3);
    let inv_input = encode_ntt_input(&params, &ntt_poly);
    group.bench_function("ntt_inv", |b| {
        b.iter(|| ntt_inv_precompile(black_box(&inv_input)))
    });

    let a: Vec<BigUint> = (0..n).map(|i| BigUint::from((i as u64 * 37) % q)).collect();
    let b_vec: Vec<BigUint> = (0..n).map(|i| BigUint::from((i as u64 * 53) % q)).collect();
    let mul_input = encode_vec_input(&BigUint::from(q), n, &a, &b_vec);
    group.bench_function("vecmulmod", |b| {
        b.iter(|| ntt_vecmulmod_precompile(black_box(&mul_input)))
    });

    let add_input = encode_vec_input(&BigUint::from(q), n, &a, &b_vec);
    group.bench_function("vecaddmod", |b| {
        b.iter(|| ntt_vecaddmod_precompile(black_box(&add_input)))
    });

    group.finish();
}

fn bench_falcon_verify_precompile(c: &mut Criterion) {
    // Build a realistic precompile input
    let params = FastNttParams::new(12289, 512, 49).unwrap();
    let s2: Vec<u64> = (0..512).map(|i| ((i as i64 * 3 % 5 - 2).rem_euclid(12289)) as u64).collect();
    let h: Vec<u64> = (0..512).map(|i| (i * 13 + 1) % 12289).collect();
    let ntth = ntt_fw_fast(&h, &params);

    let salt_msg = b"test salt 40 bytes padding xxxxxxxxxxxxxxxmsg";
    let s2c = falcon::pack(&s2);
    let ntthc = falcon::pack(&ntth);

    // Build precompile input: salt_msg_len(32) | s2(1024) | ntth(1024) | salt_msg
    let mut input = vec![0u8; 32];
    let sm_len = salt_msg.len() as u64;
    input[24..32].copy_from_slice(&sm_len.to_be_bytes());
    input.extend_from_slice(&s2c);
    input.extend_from_slice(&ntthc);
    input.extend_from_slice(salt_msg);

    c.bench_function("falcon_verify_precompile", |b| {
        b.iter(|| falcon::falcon_verify_precompile(black_box(&input)))
    });
}
