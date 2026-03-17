package vm

import (
	"encoding/binary"

	"github.com/Giulio2002/pq-eth-precompiles/go/ntt"
	"golang.org/x/crypto/sha3"
)

// NTT precompiles for Fusaka (Osaka EL fork).
// Addresses 0x12–0x15 for forward NTT, inverse NTT, vector mul mod, vector add mod.



// readN extracts the polynomial dimension n from calldata header.
// Layout: n(32 bytes BE) | ...
func readN(input []byte) uint64 {
	if len(input) < 32 {
		return 0
	}
	return binary.BigEndian.Uint64(input[24:32])
}

// nttGas computes gas for NTT_FW / NTT_INV.
// Calibrated against Apple M4 benchmarks targeting 350 Mgas/s.
// NTT_FW: 650 + 12*n  (table construction dominates base cost)
// NTT_INV: 13*n        (slightly cheaper base)
func nttGas(input []byte, base, perN uint64) uint64 {
	n := readN(input)
	if n == 0 {
		return 0
	}
	return base + perN*n
}

type nttFw struct{}

func (c *nttFw) RequiredGas(input []byte) uint64 { return nttGas(input, 650, 12) }

func (c *nttFw) Run(input []byte) ([]byte, error) {
	return ntt.NttFwPrecompile(input)
}

func (c *nttFw) Name() string { return "NTT_FW" }

type nttInv struct{}

func (c *nttInv) RequiredGas(input []byte) uint64 { return nttGas(input, 0, 13) }

func (c *nttInv) Run(input []byte) ([]byte, error) {
	return ntt.NttInvPrecompile(input)
}

func (c *nttInv) Name() string { return "NTT_INV" }

type nttVecMulMod struct{}

func (c *nttVecMulMod) RequiredGas(input []byte) uint64 { return 300 + 2*readN(input) }

func (c *nttVecMulMod) Run(input []byte) ([]byte, error) {
	return ntt.VecMulModPrecompile(input)
}

func (c *nttVecMulMod) Name() string { return "NTT_VECMULMOD" }

type nttVecAddMod struct{}

func (c *nttVecAddMod) RequiredGas(input []byte) uint64 { return 600 + readN(input) }

func (c *nttVecAddMod) Run(input []byte) ([]byte, error) {
	return ntt.VecAddModPrecompile(input)
}

func (c *nttVecAddMod) Name() string { return "NTT_VECADDMOD" }

// ── VECSUBMOD (address 0x19) ──

type nttVecSubMod struct{}

func (c *nttVecSubMod) RequiredGas(input []byte) uint64 { return 600 + readN(input) }
func (c *nttVecSubMod) Run(input []byte) ([]byte, error) {
	return ntt.VecSubModPrecompile(input)
}
func (c *nttVecSubMod) Name() string { return "NTT_VECSUBMOD" }

// ── EXPAND_A_VECMUL (address 0x1a) ──
// Expands A from rho via SHAKE128, then computes A × z in NTT domain.
// Input: q(32) | n(32) | k(32) | l(32) | rho(32) | z(l*n*cb)

type expandAVecMulPrecompile struct{}

func (c *expandAVecMulPrecompile) RequiredGas(input []byte) uint64 {
	if len(input) < 160 {
		return 0
	}
	n := binary.BigEndian.Uint64(input[56:64])
	k := binary.BigEndian.Uint64(input[88:96])
	l := binary.BigEndian.Uint64(input[120:128])
	// ExpandA: k*l SHAKE128 calls (~150 gas each) + k*l multiplies + k*(l-1) adds
	expandCost := 150 * k * l
	mulCost := (300 + 2*n) * k * l
	addCost := (600 + n) * k * (l - 1)
	return expandCost + mulCost + addCost
}
func (c *expandAVecMulPrecompile) Run(input []byte) ([]byte, error) {
	return ntt.ExpandAVecMulPrecompile(input)
}
func (c *expandAVecMulPrecompile) Name() string { return "EXPAND_A_VECMUL" }

// ── DILITHIUM_VERIFY (address 0x1b) ──
// Full ML-DSA-44 signature verification.
// Input: pk(1312) | sig(2420) | msg(var)
// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)

type dilithiumVerifyPrecompile struct{}

func (c *dilithiumVerifyPrecompile) RequiredGas(_ []byte) uint64 { return 5000 }
func (c *dilithiumVerifyPrecompile) Run(input []byte) ([]byte, error) {
	return ntt.DilithiumVerify(input)
}
func (c *dilithiumVerifyPrecompile) Name() string { return "DILITHIUM_VERIFY" }

// ── SHAKE256 precompile (address 0x16) ──
//
// Input:  output_len(32 bytes BE) | data(...)
// Output: output_len bytes of SHAKE256(data)
// Gas:    30 + 6 * ceil(len(data) / 32)  (same as KECCAK256)

type shake256Precompile struct{}

func (c *shake256Precompile) RequiredGas(input []byte) uint64 {
	// data starts after the 32-byte output_len word
	dataLen := 0
	if len(input) > 32 {
		dataLen = len(input) - 32
	}
	// 150 base + 3 per Keccak sponge block (rate=136 bytes for SHAKE256)
	blocks := uint64((dataLen + 135) / 136)
	return 150 + 3*blocks
}

func (c *shake256Precompile) Run(input []byte) ([]byte, error) {
	if len(input) < 32 {
		return nil, nil
	}
	outLen := binary.BigEndian.Uint64(input[24:32])
	if outLen == 0 || outLen > 65536 {
		return nil, nil
	}
	data := input[32:]

	h := sha3.NewShake256()
	h.Write(data)
	out := make([]byte, outLen)
	h.Read(out)
	return out, nil
}

func (c *shake256Precompile) Name() string { return "SHAKE256" }

// ── FALCON_VERIFY (0x17) ──
// Full Falcon-512 signature verification.
// Input: s2(1024, 512×uint16 BE) | ntth(1024, 512×uint16 BE) | salt_msg(var)
// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)
// Like bn256Pairing: fixed-size arrays in, 32-byte bool out.

type falconVerifyPrecompile struct{}

func (c *falconVerifyPrecompile) RequiredGas(_ []byte) uint64 { return 3100 }
func (c *falconVerifyPrecompile) Run(input []byte) ([]byte, error) {
	return ntt.FalconVerify(input)
}
func (c *falconVerifyPrecompile) Name() string { return "FALCON_VERIFY" }

// ── LP_NORM (0x18) ──
// Generalized centered Lp norm check for lattice-based signatures.
// Input: q(32) | n(32) | bound(32) | cb(32) | p(32) | s1(n×cb) | s2(n×cb) | hashed(n×cb)
// p=1: L1, p=2: L2 (squared), p=maxuint64: L∞. Only these three valid.
// Output: 32 bytes (0x00..01 valid, 0x00..00 invalid)

type lpNormPrecompile struct{}

func (c *lpNormPrecompile) RequiredGas(input []byte) uint64 {
	// n is at bytes 32..63 (second 32-byte word), header is now 5 words (160 bytes)
	if len(input) < 64 {
		return 200
	}
	n := binary.BigEndian.Uint64(input[56:64])
	return 200 + n
}
func (c *lpNormPrecompile) Run(input []byte) ([]byte, error) {
	return ntt.LpNorm(input)
}
func (c *lpNormPrecompile) Name() string { return "LP_NORM" }
