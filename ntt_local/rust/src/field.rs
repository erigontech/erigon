use num_bigint::BigUint;
use num_integer::Integer;
use num_traits::{One, Zero};

#[derive(Clone, Debug)]
pub struct FieldParams {
    pub q: BigUint,
    pub n: usize,
    pub psi: BigUint,
}

impl FieldParams {
    /// Create and validate field parameters.
    pub fn new(q: BigUint, n: usize, psi: BigUint) -> Result<Self, &'static str> {
        if !n.is_power_of_two() || n == 0 {
            return Err("n must be a power of 2");
        }

        let two_n = BigUint::from(2u64 * n as u64);
        if !q.is_odd() || &q % &two_n != BigUint::one() {
            // q must be prime and q ≡ 1 (mod 2n). We check the congruence;
            // full primality testing is left to the caller.
            return Err("q must satisfy q ≡ 1 (mod 2n)");
        }

        // Verify ψ^(2n) ≡ 1 (mod q) and ψ^n ≡ q-1 (mod q) (i.e. ψ is a primitive 2n-th root)
        let psi_2n = mod_pow(&psi, &two_n, &q);
        if psi_2n != BigUint::one() {
            return Err("psi is not a 2n-th root of unity");
        }
        let psi_n = mod_pow(&psi, &BigUint::from(n), &q);
        let q_minus_1 = &q - BigUint::one();
        if psi_n != q_minus_1 {
            return Err("psi is not a primitive 2n-th root of unity");
        }

        Ok(Self { q, n, psi })
    }

    /// Compute ω = ψ² (the n-th root of unity).
    pub fn omega(&self) -> BigUint {
        mod_pow(&self.psi, &BigUint::from(2u64), &self.q)
    }

    /// Compute ψ⁻¹ mod q.
    pub fn psi_inv(&self) -> BigUint {
        mod_inv(&self.psi, &self.q)
    }

    /// Compute n⁻¹ mod q.
    pub fn n_inv(&self) -> BigUint {
        mod_inv(&BigUint::from(self.n), &self.q)
    }

    /// Build the Ψ_rev table: powers of ψ in bit-reversed order.
    /// Table has `n` entries, indexed [0..n).
    pub fn psi_rev_table(&self) -> Vec<BigUint> {
        let n = self.n;
        let mut table = vec![BigUint::zero(); n];
        // table[i] = psi^(bitrev(i, log2(n))) for the NTT butterfly indexing.
        // Following the standard CT-butterfly convention:
        // We store psi^(bit_reverse(k)) for k in [0..n).
        let log_n = n.trailing_zeros() as usize;
        for i in 0..n {
            let rev = bit_reverse(i, log_n);
            table[i] = mod_pow(&self.psi, &BigUint::from(rev), &self.q);
        }
        table
    }

    /// Build the Ψ⁻¹_rev table: powers of ψ⁻¹ in bit-reversed order.
    pub fn psi_inv_rev_table(&self) -> Vec<BigUint> {
        let n = self.n;
        let psi_inv = self.psi_inv();
        let log_n = n.trailing_zeros() as usize;
        let mut table = vec![BigUint::zero(); n];
        for i in 0..n {
            let rev = bit_reverse(i, log_n);
            table[i] = mod_pow(&psi_inv, &BigUint::from(rev), &self.q);
        }
        table
    }

    /// Byte length of a single coefficient (ceil(log2(q) / 8)).
    pub fn coeff_byte_len(&self) -> usize {
        (self.q.bits() as usize + 7) / 8
    }
}

/// Modular exponentiation: base^exp mod modulus.
pub fn mod_pow(base: &BigUint, exp: &BigUint, modulus: &BigUint) -> BigUint {
    base.modpow(exp, modulus)
}

/// Modular inverse via Fermat's little theorem (modulus must be prime): a^(p-2) mod p.
pub fn mod_inv(a: &BigUint, p: &BigUint) -> BigUint {
    let exp = p - BigUint::from(2u64);
    a.modpow(&exp, p)
}

/// Modular addition: (a + b) mod q.
pub fn mod_add(a: &BigUint, b: &BigUint, q: &BigUint) -> BigUint {
    (a + b) % q
}

/// Modular subtraction: (a - b) mod q  (always non-negative).
pub fn mod_sub(a: &BigUint, b: &BigUint, q: &BigUint) -> BigUint {
    if a >= b {
        (a - b) % q
    } else {
        q - ((b - a) % q)
    }
}

/// Modular multiplication: (a * b) mod q.
pub fn mod_mul(a: &BigUint, b: &BigUint, q: &BigUint) -> BigUint {
    (a * b) % q
}

/// Bit-reverse an index `x` of `bits` bit-width.
pub fn bit_reverse(x: usize, bits: usize) -> usize {
    let mut result = 0usize;
    let mut val = x;
    for _ in 0..bits {
        result = (result << 1) | (val & 1);
        val >>= 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_reverse() {
        assert_eq!(bit_reverse(0b000, 3), 0b000);
        assert_eq!(bit_reverse(0b001, 3), 0b100);
        assert_eq!(bit_reverse(0b011, 3), 0b110);
        assert_eq!(bit_reverse(0b101, 3), 0b101);
    }

    #[test]
    fn test_mod_sub() {
        let q = BigUint::from(7u64);
        assert_eq!(mod_sub(&BigUint::from(5u64), &BigUint::from(3u64), &q), BigUint::from(2u64));
        assert_eq!(mod_sub(&BigUint::from(3u64), &BigUint::from(5u64), &q), BigUint::from(5u64));
    }

    #[test]
    fn test_mod_inv() {
        let q = BigUint::from(7u64);
        let a = BigUint::from(3u64);
        let inv = mod_inv(&a, &q);
        assert_eq!((&a * &inv) % &q, BigUint::one());
    }

    #[test]
    fn test_falcon_params() {
        let q = BigUint::from(12289u64);
        let n: usize = 512;
        let g = BigUint::from(11u64);
        let exp = (&q - BigUint::one()) / BigUint::from(2u64 * n as u64);
        let psi = mod_pow(&g, &exp, &q);
        let params = FieldParams::new(q, n, psi);
        assert!(params.is_ok());
    }
}
