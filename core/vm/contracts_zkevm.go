// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/crypto/blake2b"
	"github.com/ledgerwatch/erigon/crypto/bls12381"
	"github.com/ledgerwatch/erigon/params"
	"golang.org/x/crypto/ripemd160"
	//lint:ignore SA1019 Needed for precompile
)

type PrecompiledContract_zkEvm interface {
	RequiredGas(input []byte) uint64  // RequiredPrice calculates the contract gas use
	Run(input []byte) ([]byte, error) // Run runs the precompiled contract
	SetCounterCollector(cc *CounterCollector)
	SetOutputLength(outLength int)
}

// PrecompiledContractsForkID5Dragonfruit contains the default set of pre-compiled ForkID5 Dragonfruit
var PrecompiledContractsForkID5Dragonfruit = map[libcommon.Address]PrecompiledContract_zkEvm{
	libcommon.BytesToAddress([]byte{1}): &ecrecover_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{2}): &sha256hash_zkevm{},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash_zkevm{},
	libcommon.BytesToAddress([]byte{4}): &dataCopy_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{5}): &bigModExp_zkevm{eip2565: true},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul_zkevm{},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul_zkevm{},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul_zkevm{},
	libcommon.BytesToAddress([]byte{9}): &blake2F_zkevm{},
}

// PrecompiledContractForkID7Etrog contains the default set of pre-compiled ForkID7 Etrog.
var PrecompiledContractForkID7Etrog = map[libcommon.Address]PrecompiledContract_zkEvm{
	libcommon.BytesToAddress([]byte{1}): &ecrecover_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{2}): &sha256hash_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash_zkevm{enabled: false},
	libcommon.BytesToAddress([]byte{4}): &dataCopy_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{5}): &bigModExp_zkevm{enabled: true, eip2565: true},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{9}): &blake2F_zkevm{enabled: false},
}

// PrecompiledContractsForkID8 contains the default set of pre-compiled ForkID8.
var PrecompiledContractsForkID8Elderberry = map[libcommon.Address]PrecompiledContract_zkEvm{
	libcommon.BytesToAddress([]byte{1}): &ecrecover_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{2}): &sha256hash_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{3}): &ripemd160hash_zkevm{enabled: false},
	libcommon.BytesToAddress([]byte{4}): &dataCopy_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{5}): &bigModExp_zkevm{enabled: false, eip2565: true},
	libcommon.BytesToAddress([]byte{6}): &bn256AddIstanbul_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{7}): &bn256ScalarMulIstanbul_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{8}): &bn256PairingIstanbul_zkevm{enabled: true},
	libcommon.BytesToAddress([]byte{9}): &blake2F_zkevm{enabled: false},
}

// ECRECOVER implemented as a native contract.
type ecrecover_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *ecrecover_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *ecrecover_zkevm) SetOutputLength(outLength int) {
}
func (c *ecrecover_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	return params.EcrecoverGas
}

func (c *ecrecover_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	const ecRecoverInputLength = 128

	// [zkevm] - this was a bug prior to forkId6
	// this is the address that belongs to pvtKey = 0
	// occurs on testnet block number 2963608
	if fmt.Sprintf("%x", input) == "0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000001bc6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee57fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a1" {
		return libcommon.HexToHash("0x3f17f1962b36e491b30a40b2405849e597ba5fb5").Bytes(), nil
	}
	input = common.RightPadBytes(input, ecRecoverInputLength)
	// "input" is (hash, v, r, s), each 32 bytes
	// but for ecrecover we want (r, s, v)

	r := new(uint256.Int).SetBytes(input[64:96])
	s := new(uint256.Int).SetBytes(input[96:128])
	v := input[63] - 27

	if c.cc != nil {
		c.cc.preEcRecover(new(uint256.Int).SetBytes([]byte{v}), r, s)
	}

	// tighter sig s values input homestead only apply to tx sigs
	if !allZero(input[32:63]) || !crypto.ValidateSignatureValues(v, r, s, false) {
		return nil, nil
	}
	// We must make sure not to modify the 'input', so placing the 'v' along with
	// the signature needs to be done on a new allocation
	sig := make([]byte, 65)
	copy(sig, input[64:128])
	sig[64] = v
	// v needs to be at the end for libsecp256k1
	pubKey, err := crypto.Ecrecover(input[:32], sig)
	// make sure the public key is a valid one
	if err != nil {
		return nil, nil
	}

	// the first byte of pubkey is bitcoin heritage
	return common.LeftPadBytes(crypto.Keccak256(pubKey[1:])[12:], 32), nil
}

// SHA256 implemented as a native contract.
type sha256hash_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *sha256hash_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *sha256hash_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *sha256hash_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}

	return uint64(len(input)+31)/32*params.Sha256PerWordGas + params.Sha256BaseGas
}
func (c *sha256hash_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}
	h := sha256.Sum256(input)

	if c.cc != nil {
		c.cc.preSha256(len(input))
	}

	return h[:], nil
}

// RIPEMD160 implemented as a native contract.
type ripemd160hash_zkevm struct {
	enabled bool
}

// ripemd160hash doesn't have counters
func (c *ripemd160hash_zkevm) SetCounterCollector(cc *CounterCollector) {
}

func (c *ripemd160hash_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *ripemd160hash_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	return uint64(len(input)+31)/32*params.Ripemd160PerWordGas + params.Ripemd160BaseGas
}
func (c *ripemd160hash_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	ripemd := ripemd160.New()
	ripemd.Write(input)
	return common.LeftPadBytes(ripemd.Sum(nil), 32), nil
}

// data copy implemented as a native contract.
type dataCopy_zkevm struct {
	enabled   bool
	cc        *CounterCollector
	outLength int
}

func (c *dataCopy_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *dataCopy_zkevm) SetOutputLength(outLength int) {
	c.outLength = outLength
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *dataCopy_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	return uint64(len(input)+31)/32*params.IdentityPerWordGas + params.IdentityBaseGas
}
func (c *dataCopy_zkevm) Run(in []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	if c.cc != nil {
		c.cc.preIdentity(len(in), c.outLength)
	}

	return in, nil
}

// bigModExp implements a native big integer exponential modular operation.
type bigModExp_zkevm struct {
	eip2565 bool
	enabled bool
	cc      *CounterCollector
	outLen  int
}

func (c *bigModExp_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bigModExp_zkevm) SetOutputLength(outLength int) {
	c.outLen = outLength
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bigModExp_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	var (
		baseLen = new(big.Int).SetBytes(getData(input, 0, 32))
		expLen  = new(big.Int).SetBytes(getData(input, 32, 32))
		modLen  = new(big.Int).SetBytes(getData(input, 64, 32))
	)
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}
	// Retrieve the head 32 bytes of exp for the adjusted exponent length
	var expHead *big.Int
	if big.NewInt(int64(len(input))).Cmp(baseLen) <= 0 {
		expHead = new(big.Int)
	} else {
		if expLen.Cmp(big32) > 0 {
			expHead = new(big.Int).SetBytes(getData(input, baseLen.Uint64(), 32))
		} else {
			expHead = new(big.Int).SetBytes(getData(input, baseLen.Uint64(), expLen.Uint64()))
		}
	}
	// Calculate the adjusted exponent length
	var msb int
	if bitlen := expHead.BitLen(); bitlen > 0 {
		msb = bitlen - 1
	}
	adjExpLen := new(big.Int)
	if expLen.Cmp(big32) > 0 {
		adjExpLen.Sub(expLen, big32)
		adjExpLen.Mul(big8, adjExpLen)
	}
	adjExpLen.Add(adjExpLen, big.NewInt(int64(msb)))
	// Calculate the gas cost of the operation
	gas := new(big.Int).Set(math.BigMax(modLen, baseLen))
	if c.eip2565 {
		// EIP-2565 has three changes
		// 1. Different multComplexity (inlined here)
		// in EIP-2565 (https://eips.ethereum.org/EIPS/eip-2565):
		//
		// def mult_complexity(x):
		//    ceiling(x/8)^2
		//
		//where is x is max(length_of_MODULUS, length_of_BASE)
		gas = gas.Add(gas, big7)
		gas = gas.Div(gas, big8)
		gas.Mul(gas, gas)

		gas.Mul(gas, math.BigMax(adjExpLen, big1))
		// 2. Different divisor (`GQUADDIVISOR`) (3)
		gas.Div(gas, big3)
		if gas.BitLen() > 64 {
			return math.MaxUint64
		}
		// 3. Minimum price of 200 gas
		if gas.Uint64() < 200 {
			return 200
		}
		return gas.Uint64()
	}
	gas = modexpMultComplexity(gas)
	gas.Mul(gas, math.BigMax(adjExpLen, big1))
	gas.Div(gas, big20)

	if gas.BitLen() > 64 {
		return math.MaxUint64
	}
	return gas.Uint64()
}

func (c *bigModExp_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	var (
		baseLen = new(big.Int).SetBytes(getData(input, 0, 32)).Uint64()
		expLen  = new(big.Int).SetBytes(getData(input, 32, 32)).Uint64()
		modLen  = new(big.Int).SetBytes(getData(input, 64, 32)).Uint64()
	)
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}
	// Handle a special case when both the base and mod length is zero
	if baseLen == 0 && modLen == 0 {
		return []byte{}, nil
	}
	// Retrieve the operands and execute the exponentiation
	var (
		base = new(big.Int).SetBytes(getData(input, 0, baseLen))
		exp  = new(big.Int).SetBytes(getData(input, baseLen, expLen))
		mod  = new(big.Int).SetBytes(getData(input, baseLen+expLen, modLen))
		v    []byte
	)
	switch {
	case mod.BitLen() == 0:
		// Modulo 0 is undefined, return zero
		return common.LeftPadBytes([]byte{}, int(modLen)), nil
	case base.Cmp(libcommon.Big1) == 0:
		//If base == 1, then we can just return base % mod (if mod >= 1, which it is)
		v = base.Mod(base, mod).Bytes()
	//case mod.Bit(0) == 0:
	//	// Modulo is even
	//	v = math.FastExp(base, exp, mod).Bytes()
	default:
		// Modulo is odd
		v = base.Exp(base, exp, mod).Bytes()
	}

	if c.cc != nil {
		c.cc.preModExp(len(input), c.outLen, int(baseLen), int(modLen), int(expLen), base, exp, mod)
	}
	return common.LeftPadBytes(v, int(modLen)), nil
}

// bn256Add implements a native elliptic curve point addition conforming to
// Istanbul consensus rules.
type bn256AddIstanbul_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *bn256AddIstanbul_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bn256AddIstanbul_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddIstanbul_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	return params.Bn256AddGasIstanbul
}

func (c *bn256AddIstanbul_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	//increment the counters
	if c.cc != nil {
		c.cc.preECAdd()
	}
	return runBn256Add(input)
}

// bn256AddByzantium implements a native elliptic curve point addition
// conforming to Byzantium consensus rules.
type bn256AddByzantium_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *bn256AddByzantium_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bn256AddByzantium_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddByzantium_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}

	return params.Bn256AddGasByzantium
}

func (c *bn256AddByzantium_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	//increment the counters
	if c.cc != nil {
		c.cc.preECAdd()
	}
	return runBn256Add(input)
}

// bn256ScalarMulIstanbul implements a native elliptic curve scalar
// multiplication conforming to Istanbul consensus rules.
type bn256ScalarMulIstanbul_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *bn256ScalarMulIstanbul_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bn256ScalarMulIstanbul_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulIstanbul_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}

	return params.Bn256ScalarMulGasIstanbul
}

func (c *bn256ScalarMulIstanbul_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	//increment the counters
	if c.cc != nil {
		c.cc.preECMul()
	}
	return runBn256ScalarMul(input)
}

// bn256ScalarMulByzantium implements a native elliptic curve scalar
// multiplication conforming to Byzantium consensus rules.
type bn256ScalarMulByzantium_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *bn256ScalarMulByzantium_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bn256ScalarMulByzantium_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulByzantium_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	return params.Bn256ScalarMulGasByzantium
}

func (c *bn256ScalarMulByzantium_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	//increment the counters
	if c.cc != nil {
		c.cc.preECMul()
	}
	return runBn256ScalarMul(input)
}

// bn256PairingIstanbul implements a pairing pre-compile for the bn256 curve
// conforming to Istanbul consensus rules.
type bn256PairingIstanbul_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *bn256PairingIstanbul_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bn256PairingIstanbul_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingIstanbul_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	return params.Bn256PairingBaseGasIstanbul + uint64(len(input)/192)*params.Bn256PairingPerPointGasIstanbul
}

func (c *bn256PairingIstanbul_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	if c.cc != nil {
		//increment the counters
		// no need to care about non-divisible-by-192, because bn128.pairing will properly fail in that case
		inputDataSize := len(input) / 192
		c.cc.preECPairing(inputDataSize)
	}
	return runBn256Pairing(input)
}

// bn256PairingByzantium implements a pairing pre-compile for the bn256 curve
// conforming to Byzantium consensus rules.
type bn256PairingByzantium_zkevm struct {
	cc *CounterCollector
}

func (c *bn256PairingByzantium_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bn256PairingByzantium_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingByzantium_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bn256PairingBaseGasByzantium + uint64(len(input)/192)*params.Bn256PairingPerPointGasByzantium
}

func (c *bn256PairingByzantium_zkevm) Run(input []byte) ([]byte, error) {
	if c.cc != nil {
		//increment the counters
		// no need to care about non-divisible-by-192, because bn128.pairing will properly fail in that case
		inputDataSize := len(input) / 192
		c.cc.preECPairing(inputDataSize)
	}
	return runBn256Pairing(input)
}

type blake2F_zkevm struct {
	enabled bool
	cc      *CounterCollector
}

func (c *blake2F_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *blake2F_zkevm) SetOutputLength(outLength int) {
}

func (c *blake2F_zkevm) RequiredGas(input []byte) uint64 {
	if !c.enabled {
		return 0
	}
	// If the input is malformed, we can't calculate the gas, return 0 and let the
	// actual call choke and fault.
	if len(input) != blake2FInputLength {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(input[0:4]))
}

func (c *blake2F_zkevm) Run(input []byte) ([]byte, error) {
	if !c.enabled {
		return []byte{}, ErrUnsupportedPrecompile
	}

	// // Make sure the input is valid (correct length and final flag)
	if len(input) != blake2FInputLength {
		return nil, errBlake2FInvalidInputLength
	}
	if input[212] != blake2FNonFinalBlockBytes && input[212] != blake2FFinalBlockBytes {
		return nil, errBlake2FInvalidFinalFlag
	}
	// Parse the input into the Blake2b call parameters
	var (
		rounds = binary.BigEndian.Uint32(input[0:4])
		final  = input[212] == blake2FFinalBlockBytes

		h [8]uint64
		m [16]uint64
		t [2]uint64
	)
	for i := 0; i < 8; i++ {
		offset := 4 + i*8
		h[i] = binary.LittleEndian.Uint64(input[offset : offset+8])
	}
	for i := 0; i < 16; i++ {
		offset := 68 + i*8
		m[i] = binary.LittleEndian.Uint64(input[offset : offset+8])
	}
	t[0] = binary.LittleEndian.Uint64(input[196:204])
	t[1] = binary.LittleEndian.Uint64(input[204:212])

	// Execute the compression function, extract and return the result
	blake2b.F(&h, m, t, final, rounds)

	output := make([]byte, 64)
	for i := 0; i < 8; i++ {
		offset := i * 8
		binary.LittleEndian.PutUint64(output[offset:offset+8], h[i])
	}
	return output, nil
}

// bls12381G1Add implements EIP-2537 G1Add precompile.
type bls12381G1Add_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381G1Add_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381G1Add_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1Add_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381G1AddGas
}

func (c *bls12381G1Add_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1Add precompile.
	// > G1 addition call expects `256` bytes as an input that is interpreted as byte concatenation of two G1 points (`128` bytes each).
	// > Output is an encoding of addition operation result - single G1 point (`128` bytes).
	if len(input) != 256 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0, p1 *bls12381.PointG1

	// Initialize G1
	g := bls12381.NewG1()

	// Decode G1 point p_0
	if p0, err = g.DecodePoint(input[:128]); err != nil {
		return nil, err
	}
	// Decode G1 point p_1
	if p1, err = g.DecodePoint(input[128:]); err != nil {
		return nil, err
	}

	// Compute r = p_0 + p_1
	r := g.New()
	g.Add(r, p0, p1)

	// Encode the G1 point result into 128 bytes
	return g.EncodePoint(r), nil
}

// bls12381G1Mul implements EIP-2537 G1Mul precompile.
type bls12381G1Mul_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381G1Mul_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381G1Mul_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1Mul_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381G1MulGas
}

func (c *bls12381G1Mul_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1Mul precompile.
	// > G1 multiplication call expects `160` bytes as an input that is interpreted as byte concatenation of encoding of G1 point (`128` bytes) and encoding of a scalar value (`32` bytes).
	// > Output is an encoding of multiplication operation result - single G1 point (`128` bytes).
	if len(input) != 160 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0 *bls12381.PointG1

	// Initialize G1
	g := bls12381.NewG1()

	// Decode G1 point
	if p0, err = g.DecodePoint(input[:128]); err != nil {
		return nil, err
	}
	// Decode scalar value
	e := new(big.Int).SetBytes(input[128:])

	// Compute r = e * p_0
	r := g.New()
	g.MulScalar(r, p0, e)

	// Encode the G1 point into 128 bytes
	return g.EncodePoint(r), nil
}

// bls12381G1MultiExp implements EIP-2537 G1MultiExp precompile.
type bls12381G1MultiExp_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381G1MultiExp_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381G1MultiExp_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1MultiExp_zkevm) RequiredGas(input []byte) uint64 {
	// Calculate G1 point, scalar value pair length
	k := len(input) / 160
	if k == 0 {
		// Return 0 gas for small input length
		return 0
	}
	// Lookup discount value for G1 point, scalar value pair length
	var discount uint64
	if dLen := len(params.Bls12381MultiExpDiscountTable); k < dLen {
		discount = params.Bls12381MultiExpDiscountTable[k-1]
	} else {
		discount = params.Bls12381MultiExpDiscountTable[dLen-1]
	}
	// Calculate gas and return the result
	return (uint64(k) * params.Bls12381G1MulGas * discount) / 1000
}

func (c *bls12381G1MultiExp_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1MultiExp precompile.
	// G1 multiplication call expects `160*k` bytes as an input that is interpreted as byte concatenation of `k` slices each of them being a byte concatenation of encoding of G1 point (`128` bytes) and encoding of a scalar value (`32` bytes).
	// Output is an encoding of multiexponentiation operation result - single G1 point (`128` bytes).
	k := len(input) / 160
	if len(input) == 0 || len(input)%160 != 0 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	points := make([]*bls12381.PointG1, k)
	scalars := make([]*big.Int, k)

	// Initialize G1
	g := bls12381.NewG1()

	// Decode point scalar pairs
	for i := 0; i < k; i++ {
		off := 160 * i
		t0, t1, t2 := off, off+128, off+160
		// Decode G1 point
		if points[i], err = g.DecodePoint(input[t0:t1]); err != nil {
			return nil, err
		}
		// Decode scalar value
		scalars[i] = new(big.Int).SetBytes(input[t1:t2])
	}

	// Compute r = e_0 * p_0 + e_1 * p_1 + ... + e_(k-1) * p_(k-1)
	r := g.New()
	if _, err = g.MultiExp(r, points, scalars); err != nil {
		return nil, err
	}

	// Encode the G1 point to 128 bytes
	return g.EncodePoint(r), nil
}

// bls12381G2Add implements EIP-2537 G2Add precompile.
type bls12381G2Add_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381G2Add_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381G2Add_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2Add_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381G2AddGas
}

func (c *bls12381G2Add_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2Add precompile.
	// > G2 addition call expects `512` bytes as an input that is interpreted as byte concatenation of two G2 points (`256` bytes each).
	// > Output is an encoding of addition operation result - single G2 point (`256` bytes).
	if len(input) != 512 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0, p1 *bls12381.PointG2

	// Initialize G2
	g := bls12381.NewG2()
	r := g.New()

	// Decode G2 point p_0
	if p0, err = g.DecodePoint(input[:256]); err != nil {
		return nil, err
	}
	// Decode G2 point p_1
	if p1, err = g.DecodePoint(input[256:]); err != nil {
		return nil, err
	}

	// Compute r = p_0 + p_1
	g.Add(r, p0, p1)

	// Encode the G2 point into 256 bytes
	return g.EncodePoint(r), nil
}

// bls12381G2Mul implements EIP-2537 G2Mul precompile.
type bls12381G2Mul_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381G2Mul_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381G2Mul_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2Mul_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381G2MulGas
}

func (c *bls12381G2Mul_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2MUL precompile logic.
	// > G2 multiplication call expects `288` bytes as an input that is interpreted as byte concatenation of encoding of G2 point (`256` bytes) and encoding of a scalar value (`32` bytes).
	// > Output is an encoding of multiplication operation result - single G2 point (`256` bytes).
	if len(input) != 288 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0 *bls12381.PointG2

	// Initialize G2
	g := bls12381.NewG2()

	// Decode G2 point
	if p0, err = g.DecodePoint(input[:256]); err != nil {
		return nil, err
	}
	// Decode scalar value
	e := new(big.Int).SetBytes(input[256:])

	// Compute r = e * p_0
	r := g.New()
	g.MulScalar(r, p0, e)

	// Encode the G2 point into 256 bytes
	return g.EncodePoint(r), nil
}

// bls12381G2MultiExp implements EIP-2537 G2MultiExp precompile.
type bls12381G2MultiExp_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381G2MultiExp_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381G2MultiExp_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2MultiExp_zkevm) RequiredGas(input []byte) uint64 {
	// Calculate G2 point, scalar value pair length
	k := len(input) / 288
	if k == 0 {
		// Return 0 gas for small input length
		return 0
	}
	// Lookup discount value for G2 point, scalar value pair length
	var discount uint64
	if dLen := len(params.Bls12381MultiExpDiscountTable); k < dLen {
		discount = params.Bls12381MultiExpDiscountTable[k-1]
	} else {
		discount = params.Bls12381MultiExpDiscountTable[dLen-1]
	}
	// Calculate gas and return the result
	return (uint64(k) * params.Bls12381G2MulGas * discount) / 1000
}

func (c *bls12381G2MultiExp_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2MultiExp precompile logic
	// > G2 multiplication call expects `288*k` bytes as an input that is interpreted as byte concatenation of `k` slices each of them being a byte concatenation of encoding of G2 point (`256` bytes) and encoding of a scalar value (`32` bytes).
	// > Output is an encoding of multiexponentiation operation result - single G2 point (`256` bytes).
	k := len(input) / 288
	if len(input) == 0 || len(input)%288 != 0 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	points := make([]*bls12381.PointG2, k)
	scalars := make([]*big.Int, k)

	// Initialize G2
	g := bls12381.NewG2()

	// Decode point scalar pairs
	for i := 0; i < k; i++ {
		off := 288 * i
		t0, t1, t2 := off, off+256, off+288
		// Decode G1 point
		if points[i], err = g.DecodePoint(input[t0:t1]); err != nil {
			return nil, err
		}
		// Decode scalar value
		scalars[i] = new(big.Int).SetBytes(input[t1:t2])
	}

	// Compute r = e_0 * p_0 + e_1 * p_1 + ... + e_(k-1) * p_(k-1)
	r := g.New()
	if _, err := g.MultiExp(r, points, scalars); err != nil {
		return nil, err
	}

	// Encode the G2 point to 256 bytes.
	return g.EncodePoint(r), nil
}

// bls12381Pairing implements EIP-2537 Pairing precompile.
type bls12381Pairing_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381Pairing_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381Pairing_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381Pairing_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381PairingBaseGas + uint64(len(input)/384)*params.Bls12381PairingPerPairGas
}

func (c *bls12381Pairing_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Pairing precompile logic.
	// > Pairing call expects `384*k` bytes as an inputs that is interpreted as byte concatenation of `k` slices. Each slice has the following structure:
	// > - `128` bytes of G1 point encoding
	// > - `256` bytes of G2 point encoding
	// > Output is a `32` bytes where last single byte is `0x01` if pairing result is equal to multiplicative identity in a pairing target field and `0x00` otherwise
	// > (which is equivalent of Big Endian encoding of Solidity values `uint256(1)` and `uin256(0)` respectively).
	k := len(input) / 384
	if len(input) == 0 || len(input)%384 != 0 {
		return nil, errBLS12381InvalidInputLength
	}

	// Initialize BLS12-381 pairing engine
	e := bls12381.NewPairingEngine()
	g1, g2 := e.G1, e.G2

	// Decode pairs
	for i := 0; i < k; i++ {
		off := 384 * i
		t0, t1, t2 := off, off+128, off+384

		// Decode G1 point
		p1, err := g1.DecodePoint(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Decode G2 point
		p2, err := g2.DecodePoint(input[t1:t2])
		if err != nil {
			return nil, err
		}

		// 'point is on curve' check already done,
		// Here we need to apply subgroup checks.
		if !g1.InCorrectSubgroup(p1) {
			return nil, errBLS12381G1PointSubgroup
		}
		if !g2.InCorrectSubgroup(p2) {
			return nil, errBLS12381G2PointSubgroup
		}

		// Update pairing engine with G1 and G2 ponits
		e.AddPair(p1, p2)
	}
	// Prepare 32 byte output
	out := make([]byte, 32)

	// Compute pairing and set the result
	if e.Check() {
		out[31] = 1
	}
	return out, nil
}

// bls12381MapG1 implements EIP-2537 MapG1 precompile.
type bls12381MapG1_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381MapG1_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381MapG1_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381MapG1_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381MapG1Gas
}

func (c *bls12381MapG1_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Map_To_G1 precompile.
	// > Field-to-curve call expects `64` bytes an an input that is interpreted as a an element of the base field.
	// > Output of this call is `128` bytes and is G1 point following respective encoding rules.
	if len(input) != 64 {
		return nil, errBLS12381InvalidInputLength
	}

	// Decode input field element
	fe, err := decodeBLS12381FieldElement(input)
	if err != nil {
		return nil, err
	}

	// Initialize G1
	g := bls12381.NewG1()

	// Compute mapping
	r, err := g.MapToCurve(fe)
	if err != nil {
		return nil, err
	}

	// Encode the G1 point to 128 bytes
	return g.EncodePoint(r), nil
}

// bls12381MapG2 implements EIP-2537 MapG2 precompile.
type bls12381MapG2_zkevm struct {
	cc *CounterCollector
}

func (c *bls12381MapG2_zkevm) SetCounterCollector(cc *CounterCollector) {
	c.cc = cc
}

func (c *bls12381MapG2_zkevm) SetOutputLength(outLength int) {
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381MapG2_zkevm) RequiredGas(input []byte) uint64 {
	return params.Bls12381MapG2Gas
}

func (c *bls12381MapG2_zkevm) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Map_FP2_TO_G2 precompile logic.
	// > Field-to-curve call expects `128` bytes an an input that is interpreted as a an element of the quadratic extension field.
	// > Output of this call is `256` bytes and is G2 point following respective encoding rules.
	if len(input) != 128 {
		return nil, errBLS12381InvalidInputLength
	}

	// Decode input field element
	fe := make([]byte, 96)
	c0, err := decodeBLS12381FieldElement(input[:64])
	if err != nil {
		return nil, err
	}
	copy(fe[48:], c0)
	c1, err := decodeBLS12381FieldElement(input[64:])
	if err != nil {
		return nil, err
	}
	copy(fe[:48], c1)

	// Initialize G2
	g := bls12381.NewG2()

	// Compute mapping
	r, err := g.MapToCurve(fe)
	if err != nil {
		return nil, err
	}

	// Encode the G2 point to 256 bytes
	return g.EncodePoint(r), nil
}
