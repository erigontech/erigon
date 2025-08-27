// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"
	"math/bits"

	"github.com/consensys/gnark-crypto/ecc"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fp"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/consensys/gnark-crypto/ecc/bn254"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/crypto/blake2b"
	libbn254 "github.com/erigontech/erigon-lib/crypto/bn254"
	libkzg "github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/crypto/secp256r1"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"

	//lint:ignore SA1019 Needed for precompile
	"golang.org/x/crypto/ripemd160"
)

// PrecompiledContract is the basic interface for native Go contracts. The implementation
// requires a deterministic gas count based on the input size of the Run method of the
// contract.
type PrecompiledContract interface {
	RequiredGas(input []byte) uint64  // RequiredPrice calculates the contract gas use
	Run(input []byte) ([]byte, error) // Run runs the precompiled contract
	Name() string
}

func Precompiles(chainRules *chain.Rules) map[common.Address]PrecompiledContract {
	switch {
	case chainRules.IsOsaka:
		return PrecompiledContractsOsaka
	case chainRules.IsBhilai:
		return PrecompiledContractsBhilai
	case chainRules.IsPrague:
		return PrecompiledContractsPrague
	case chainRules.IsNapoli:
		return PrecompiledContractsNapoli
	case chainRules.IsCancun:
		return PrecompiledContractsCancun
	case chainRules.IsBerlin:
		return PrecompiledContractsBerlin
	case chainRules.IsIstanbul:
		return PrecompiledContractsIstanbul
	case chainRules.IsByzantium:
		return PrecompiledContractsByzantium
	default:
		return PrecompiledContractsHomestead
	}
}

// PrecompiledContractsHomestead contains the default set of pre-compiled Ethereum
// contracts used in the Frontier and Homestead releases.
var PrecompiledContractsHomestead = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{1}): &ecrecover{},
	common.BytesToAddress([]byte{2}): &sha256hash{},
	common.BytesToAddress([]byte{3}): &ripemd160hash{},
	common.BytesToAddress([]byte{4}): &dataCopy{},
}

// PrecompiledContractsByzantium contains the default set of pre-compiled Ethereum
// contracts used in the Byzantium release.
var PrecompiledContractsByzantium = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{1}): &ecrecover{},
	common.BytesToAddress([]byte{2}): &sha256hash{},
	common.BytesToAddress([]byte{3}): &ripemd160hash{},
	common.BytesToAddress([]byte{4}): &dataCopy{},
	common.BytesToAddress([]byte{5}): &bigModExp{eip2565: false},
	common.BytesToAddress([]byte{6}): &bn254AddByzantium{},
	common.BytesToAddress([]byte{7}): &bn254ScalarMulByzantium{},
	common.BytesToAddress([]byte{8}): &bn254PairingByzantium{},
}

// PrecompiledContractsIstanbul contains the default set of pre-compiled Ethereum
// contracts used in the Istanbul release.
var PrecompiledContractsIstanbul = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{1}): &ecrecover{},
	common.BytesToAddress([]byte{2}): &sha256hash{},
	common.BytesToAddress([]byte{3}): &ripemd160hash{},
	common.BytesToAddress([]byte{4}): &dataCopy{},
	common.BytesToAddress([]byte{5}): &bigModExp{eip2565: false},
	common.BytesToAddress([]byte{6}): &bn254AddIstanbul{},
	common.BytesToAddress([]byte{7}): &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{8}): &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{9}): &blake2F{},
}

// PrecompiledContractsBerlin contains the default set of pre-compiled Ethereum
// contracts used in the Berlin release.
var PrecompiledContractsBerlin = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{1}): &ecrecover{},
	common.BytesToAddress([]byte{2}): &sha256hash{},
	common.BytesToAddress([]byte{3}): &ripemd160hash{},
	common.BytesToAddress([]byte{4}): &dataCopy{},
	common.BytesToAddress([]byte{5}): &bigModExp{eip2565: true},
	common.BytesToAddress([]byte{6}): &bn254AddIstanbul{},
	common.BytesToAddress([]byte{7}): &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{8}): &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{9}): &blake2F{},
}

var PrecompiledContractsCancun = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01}): &ecrecover{},
	common.BytesToAddress([]byte{0x02}): &sha256hash{},
	common.BytesToAddress([]byte{0x03}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x04}): &dataCopy{},
	common.BytesToAddress([]byte{0x05}): &bigModExp{eip2565: true},
	common.BytesToAddress([]byte{0x06}): &bn254AddIstanbul{},
	common.BytesToAddress([]byte{0x07}): &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x08}): &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{0x09}): &blake2F{},
	common.BytesToAddress([]byte{0x0a}): &pointEvaluation{},
}

var PrecompiledContractsNapoli = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01}):       &ecrecover{},
	common.BytesToAddress([]byte{0x02}):       &sha256hash{},
	common.BytesToAddress([]byte{0x03}):       &ripemd160hash{},
	common.BytesToAddress([]byte{0x04}):       &dataCopy{},
	common.BytesToAddress([]byte{0x05}):       &bigModExp{eip2565: true},
	common.BytesToAddress([]byte{0x06}):       &bn254AddIstanbul{},
	common.BytesToAddress([]byte{0x07}):       &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x08}):       &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{0x09}):       &blake2F{},
	common.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
}

var PrecompiledContractsBhilai = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01}):       &ecrecover{},
	common.BytesToAddress([]byte{0x02}):       &sha256hash{},
	common.BytesToAddress([]byte{0x03}):       &ripemd160hash{},
	common.BytesToAddress([]byte{0x04}):       &dataCopy{},
	common.BytesToAddress([]byte{0x05}):       &bigModExp{eip2565: true},
	common.BytesToAddress([]byte{0x06}):       &bn254AddIstanbul{},
	common.BytesToAddress([]byte{0x07}):       &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x08}):       &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{0x09}):       &blake2F{},
	common.BytesToAddress([]byte{0x0b}):       &bls12381G1Add{},
	common.BytesToAddress([]byte{0x0c}):       &bls12381G1MultiExp{},
	common.BytesToAddress([]byte{0x0d}):       &bls12381G2Add{},
	common.BytesToAddress([]byte{0x0e}):       &bls12381G2MultiExp{},
	common.BytesToAddress([]byte{0x0f}):       &bls12381Pairing{},
	common.BytesToAddress([]byte{0x10}):       &bls12381MapFpToG1{},
	common.BytesToAddress([]byte{0x11}):       &bls12381MapFp2ToG2{},
	common.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
}

var PrecompiledContractsPrague = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01}): &ecrecover{},
	common.BytesToAddress([]byte{0x02}): &sha256hash{},
	common.BytesToAddress([]byte{0x03}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x04}): &dataCopy{},
	common.BytesToAddress([]byte{0x05}): &bigModExp{eip2565: true},
	common.BytesToAddress([]byte{0x06}): &bn254AddIstanbul{},
	common.BytesToAddress([]byte{0x07}): &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x08}): &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{0x09}): &blake2F{},
	common.BytesToAddress([]byte{0x0a}): &pointEvaluation{},
	common.BytesToAddress([]byte{0x0b}): &bls12381G1Add{},
	common.BytesToAddress([]byte{0x0c}): &bls12381G1MultiExp{},
	common.BytesToAddress([]byte{0x0d}): &bls12381G2Add{},
	common.BytesToAddress([]byte{0x0e}): &bls12381G2MultiExp{},
	common.BytesToAddress([]byte{0x0f}): &bls12381Pairing{},
	common.BytesToAddress([]byte{0x10}): &bls12381MapFpToG1{},
	common.BytesToAddress([]byte{0x11}): &bls12381MapFp2ToG2{},
}

var PrecompiledContractsOsaka = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01}):       &ecrecover{},
	common.BytesToAddress([]byte{0x02}):       &sha256hash{},
	common.BytesToAddress([]byte{0x03}):       &ripemd160hash{},
	common.BytesToAddress([]byte{0x04}):       &dataCopy{},
	common.BytesToAddress([]byte{0x05}):       &bigModExp{osaka: true},
	common.BytesToAddress([]byte{0x06}):       &bn254AddIstanbul{},
	common.BytesToAddress([]byte{0x07}):       &bn254ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x08}):       &bn254PairingIstanbul{},
	common.BytesToAddress([]byte{0x09}):       &blake2F{},
	common.BytesToAddress([]byte{0x0a}):       &pointEvaluation{},
	common.BytesToAddress([]byte{0x0b}):       &bls12381G1Add{},
	common.BytesToAddress([]byte{0x0c}):       &bls12381G1MultiExp{},
	common.BytesToAddress([]byte{0x0d}):       &bls12381G2Add{},
	common.BytesToAddress([]byte{0x0e}):       &bls12381G2MultiExp{},
	common.BytesToAddress([]byte{0x0f}):       &bls12381Pairing{},
	common.BytesToAddress([]byte{0x10}):       &bls12381MapFpToG1{},
	common.BytesToAddress([]byte{0x11}):       &bls12381MapFp2ToG2{},
	common.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{eip7951: true},
}

var (
	PrecompiledAddressesOsaka     []common.Address
	PrecompiledAddressesPrague    []common.Address
	PrecompiledAddressesNapoli    []common.Address
	PrecompiledAddressesBhilai    []common.Address
	PrecompiledAddressesCancun    []common.Address
	PrecompiledAddressesBerlin    []common.Address
	PrecompiledAddressesIstanbul  []common.Address
	PrecompiledAddressesByzantium []common.Address
	PrecompiledAddressesHomestead []common.Address
)

func init() {
	for k := range PrecompiledContractsHomestead {
		PrecompiledAddressesHomestead = append(PrecompiledAddressesHomestead, k)
	}
	for k := range PrecompiledContractsByzantium {
		PrecompiledAddressesByzantium = append(PrecompiledAddressesByzantium, k)
	}
	for k := range PrecompiledContractsIstanbul {
		PrecompiledAddressesIstanbul = append(PrecompiledAddressesIstanbul, k)
	}
	for k := range PrecompiledContractsBerlin {
		PrecompiledAddressesBerlin = append(PrecompiledAddressesBerlin, k)
	}
	for k := range PrecompiledContractsCancun {
		PrecompiledAddressesCancun = append(PrecompiledAddressesCancun, k)
	}
	for k := range PrecompiledContractsNapoli {
		PrecompiledAddressesNapoli = append(PrecompiledAddressesNapoli, k)
	}
	for k := range PrecompiledContractsBhilai {
		PrecompiledAddressesBhilai = append(PrecompiledAddressesBhilai, k)
	}
	for k := range PrecompiledContractsPrague {
		PrecompiledAddressesPrague = append(PrecompiledAddressesPrague, k)
	}
	for k := range PrecompiledContractsOsaka {
		PrecompiledAddressesOsaka = append(PrecompiledAddressesOsaka, k)
	}
}

// ActivePrecompiles returns the precompiles enabled with the current configuration.
func ActivePrecompiles(rules *chain.Rules) []common.Address {
	switch {
	case rules.IsOsaka:
		return PrecompiledAddressesOsaka
	case rules.IsBhilai:
		return PrecompiledAddressesBhilai
	case rules.IsPrague:
		return PrecompiledAddressesPrague
	case rules.IsNapoli:
		return PrecompiledAddressesNapoli
	case rules.IsCancun:
		return PrecompiledAddressesCancun
	case rules.IsBerlin:
		return PrecompiledAddressesBerlin
	case rules.IsIstanbul:
		return PrecompiledAddressesIstanbul
	case rules.IsByzantium:
		return PrecompiledAddressesByzantium
	default:
		return PrecompiledAddressesHomestead
	}
}

// RunPrecompiledContract runs and evaluates the output of a precompiled contract.
// It returns
// - the returned bytes,
// - the _remaining_ gas,
// - any error that occurred
func RunPrecompiledContract(p PrecompiledContract, input []byte, suppliedGas uint64, tracer *tracing.Hooks,
) (ret []byte, remainingGas uint64, err error) {
	gasCost := p.RequiredGas(input)
	if suppliedGas < gasCost {
		return nil, 0, ErrOutOfGas
	}

	if tracer != nil && tracer.OnGasChange != nil {
		tracer.OnGasChange(suppliedGas, suppliedGas-gasCost, tracing.GasChangeCallPrecompiledContract)
	}

	suppliedGas -= gasCost
	output, err := p.Run(input)
	return output, suppliedGas, err
}

// ECRECOVER implemented as a native contract.
type ecrecover struct{}

func (c *ecrecover) RequiredGas(input []byte) uint64 {
	return params.EcrecoverGas
}

func (c *ecrecover) Run(input []byte) ([]byte, error) {
	const ecRecoverInputLength = 128

	input = common.RightPadBytes(input, ecRecoverInputLength)
	// "input" is (hash, v, r, s), each 32 bytes
	// but for ecrecover we want (r, s, v)

	r := new(uint256.Int).SetBytes(input[64:96])
	s := new(uint256.Int).SetBytes(input[96:128])
	v := input[63] - 27

	// tighter sig s values input homestead only apply to txn sigs
	if !allZero(input[32:63]) || !crypto.TransactionSignatureIsValid(v, r, s, true /* allowPreEip2s */) {
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

func (c *ecrecover) Name() string {
	return "ECREC"
}

// SHA256 implemented as a native contract.
type sha256hash struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *sha256hash) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.Sha256PerWordGas + params.Sha256BaseGas
}
func (c *sha256hash) Run(input []byte) ([]byte, error) {
	h := sha256.Sum256(input)
	return h[:], nil
}

func (c *sha256hash) Name() string {
	return "SHA256"
}

// RIPEMD160 implemented as a native contract.
type ripemd160hash struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *ripemd160hash) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.Ripemd160PerWordGas + params.Ripemd160BaseGas
}
func (c *ripemd160hash) Run(input []byte) ([]byte, error) {
	ripemd := ripemd160.New()
	ripemd.Write(input)
	return common.LeftPadBytes(ripemd.Sum(nil), 32), nil
}

func (c *ripemd160hash) Name() string {
	return "RIPEMD160"
}

// data copy implemented as a native contract.
type dataCopy struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *dataCopy) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.IdentityPerWordGas + params.IdentityBaseGas
}
func (c *dataCopy) Run(in []byte) ([]byte, error) {
	return common.CopyBytes(in), nil
}

func (c *dataCopy) Name() string {
	return "ID"
}

// bigModExp implements a native big integer exponential modular operation.
type bigModExp struct {
	eip2565 bool
	osaka   bool // EIP-7823 & 7883
}

// modExpMultComplexityEip2565 implements modExp multiplication complexity formula, as defined in EIP-2565
//
// def mult_complexity(x):
//
//	words = math.ceil(x / 8)
//	return words**2
//
// where is x is max(base_length, modulus_length)
func modExpMultComplexityEip2565(x uint32) uint64 {
	numWords := (uint64(x) + 7) / 8
	return numWords * numWords
}

// modExpMultComplexityEip7883 implements modExp multiplication complexity formula, as defined in EIP-7883
//
// def mult_complexity(x):
//
// words = math.ceil(x / 8)
// multiplication_complexity = 16
// if x > 32: multiplication_complexity = 2 * words**2
// return multiplication_complexity
//
// where is x is max(base_length, modulus_length)
func modExpMultComplexityEip7883(x uint32) uint64 {
	if x > 32 {
		return modExpMultComplexityEip2565(x) * 2
	}
	return 16
}

// modExpMultComplexityEip198 implements modExp multiplication complexity formula, as defined in EIP-198
//
// def mult_complexity(x):
//
//	if x <= 64: return x ** 2
//	elif x <= 1024: return x ** 2 // 4 + 96 * x - 3072
//	else: return x ** 2 // 16 + 480 * x - 199680
//
// where is x is max(base_length, modulus_length)
func modExpMultComplexityEip198(x uint32) uint64 {
	xx := uint64(x) * uint64(x)
	switch {
	case x <= 64:
		return xx
	case x <= 1024:
		// (x ** 2 // 4 ) + ( 96 * x - 3072)
		return xx/4 + 96*uint64(x) - 3072
	default:
		// (x ** 2 // 16) + (480 * x - 199680)
		// max value: 0x100001df'dffcf220
		return xx/16 + 480*uint64(x) - 199680
	}
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bigModExp) RequiredGas(input []byte) uint64 {

	var minGas uint64
	var adjExpFactor uint64
	var finalDivisor uint64
	var calcMultComplexity func(uint32) uint64
	switch {
	case c.osaka:
		minGas = 500
		adjExpFactor = 16
		finalDivisor = 1
		calcMultComplexity = modExpMultComplexityEip7883
	case c.eip2565:
		minGas = 200
		adjExpFactor = 8
		finalDivisor = 3
		calcMultComplexity = modExpMultComplexityEip2565
	default:
		minGas = 0
		adjExpFactor = 8
		finalDivisor = 20
		calcMultComplexity = modExpMultComplexityEip198
	}

	header := getData(input, 0, 3*32)
	baseLen256 := new(uint256.Int).SetBytes32(header[0:32])
	expLen256 := new(uint256.Int).SetBytes32(header[32:64])
	modLen256 := new(uint256.Int).SetBytes32(header[64:96])
	lenLimit := uint64(math.MaxUint32)

	// If base or mod is bigger than uint32, the gas cost will be huge.
	if baseLen256.CmpUint64(lenLimit) > 0 || modLen256.CmpUint64(lenLimit) > 0 {
		return math.MaxUint64
	}

	// If exp is bigger than uint32:
	if expLen256.CmpUint64(lenLimit) > 0 {
		// Before EIP-7883, 0 multiplication complexity cancels the big exp.
		if !c.osaka && baseLen256.IsZero() && modLen256.IsZero() {
			return minGas
		}
		// Otherwise, the gas cost will be huge.
		return math.MaxUint64
	}

	var (
		baseLen = uint32(baseLen256.Uint64())
		expLen  = uint32(expLen256.Uint64())
		modLen  = uint32(modLen256.Uint64())
	)
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}
	// Retrieve the head 32 bytes of exp for the adjusted exponent length
	expHeadLen := min(expLen, 32)
	expOffset := baseLen
	var expHeadExplicitBytes []byte
	if expOffset < uint32(len(input)) {
		expHeadExplicitBytes = input[expOffset : expOffset+min(expHeadLen, uint32(len(input))-expOffset)]
	}
	// Compute the exp bit width
	expBitWidth := uint32(0)
	for i := 0; i < len(expHeadExplicitBytes); i++ {
		expByte := expHeadExplicitBytes[i]
		if expByte != 0 {
			expTopByteBitWidth := 8 - uint32(bits.LeadingZeros8(expByte))
			expBitWidth = 8*(expHeadLen-uint32(i)-1) + expTopByteBitWidth
			break
		}
	}
	// Compute the adjusted exp length
	expTailLen := expLen - expHeadLen
	expHeadBits := max(expBitWidth, 1) - 1
	adjExpLen := max(adjExpFactor*uint64(expTailLen)+uint64(expHeadBits), 1)

	maxLen := max(baseLen, modLen)
	multComplexity := calcMultComplexity(maxLen)
	gasHi, gasLo := bits.Mul64(multComplexity, adjExpLen)
	if gasHi != 0 {
		return math.MaxUint64
	}
	gas := gasLo / finalDivisor
	return max(gas, minGas)
}

var (
	errModExpBaseLengthTooLarge     = errors.New("base length is too large")
	errModExpExponentLengthTooLarge = errors.New("exponent length is too large")
	errModExpModulusLengthTooLarge  = errors.New("modulus length is too large")
)

func (c *bigModExp) Run(input []byte) ([]byte, error) {
	// TODO: This can be done without any allocation.
	header := getData(input, 0, 3*32)
	var (
		baseLen = binary.BigEndian.Uint64(header[32-8 : 32])
		expLen  = binary.BigEndian.Uint64(header[64-8 : 64])
		modLen  = binary.BigEndian.Uint64(header[96-8 : 96])

		// 32 - 8 bytes are truncated in the Uint64 conversion above
		baseLenHighBitsAreZero = allZero(header[0 : 32-8])
		expLenHighBitsAreZero  = allZero(header[32 : 64-8])
		modLenHighBitsAreZero  = allZero(header[64 : 96-8])
	)
	if c.osaka {
		// EIP-7823: Set upper bounds for MODEXP
		if !baseLenHighBitsAreZero || baseLen > 1024 {
			return nil, errModExpBaseLengthTooLarge
		}
		if !expLenHighBitsAreZero || expLen > 1024 {
			return nil, errModExpExponentLengthTooLarge
		}
		if !modLenHighBitsAreZero || modLen > 1024 {
			return nil, errModExpModulusLengthTooLarge
		}
	}

	// Handle a special case when mod length is zero
	if modLen == 0 && modLenHighBitsAreZero {
		return []byte{}, nil
	}

	if !baseLenHighBitsAreZero || !expLenHighBitsAreZero || !modLenHighBitsAreZero {
		return nil, ErrOutOfGas
	}

	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}
	// Retrieve the operands and execute the exponentiation
	var (
		base = new(big.Int).SetBytes(getData(input, 0, baseLen))
		exp  = new(big.Int).SetBytes(getData(input, baseLen, expLen))
		mod  = new(big.Int).SetBytes(getData(input, baseLen+expLen, modLen))
		v    []byte
	)
	switch {
	case mod.Cmp(common.Big1) <= 0:
		// Leave the result as zero for mod 0 (undefined) and 1
	case base.Cmp(common.Big1) == 0:
		// If base == 1 (and mod > 1), then the result is 1
		v = common.Big1.Bytes()
	default:
		v = base.Exp(base, exp, mod).Bytes()
	}
	return common.LeftPadBytes(v, int(modLen)), nil
}

func (c *bigModExp) Name() string {
	return "MODEXP"
}

// newCurvePoint unmarshals a binary blob into a bn254 elliptic curve point,
// returning it, or an error if the point is invalid.
func newCurvePoint(blob []byte) (*libbn254.G1, error) {
	p := new(libbn254.G1)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

// newTwistPoint unmarshals a binary blob into a bn254 elliptic curve point,
// returning it, or an error if the point is invalid.
func newTwistPoint(blob []byte) (*libbn254.G2, error) {
	p := new(libbn254.G2)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

// runBn254Add implements the Bn254Add precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn254Add(input []byte) ([]byte, error) {
	x := bn254.G1Affine{}
	err := libbn254.UnmarshalCurvePointG1(getData(input, 0, 64), &x)
	if err != nil {
		return nil, err
	}

	y := bn254.G1Affine{}
	err = libbn254.UnmarshalCurvePointG1(getData(input, 64, 64), &y)
	if err != nil {
		return nil, err
	}
	return libbn254.MarshalCurvePointG1(x.Add(&x, &y)), nil
}

// bn254Add implements a native elliptic curve point addition conforming to
// Istanbul consensus rules.
type bn254AddIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn254AddIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn254AddGasIstanbul
}

func (c *bn254AddIstanbul) Run(input []byte) ([]byte, error) {
	return runBn254Add(input)
}

func (c *bn254AddIstanbul) Name() string {
	return "BN254_ADD" // note bn254 is the correct name and is required by eth_config
}

// bn254AddByzantium implements a native elliptic curve point addition
// conforming to Byzantium consensus rules.
type bn254AddByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn254AddByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn254AddGasByzantium
}

func (c *bn254AddByzantium) Run(input []byte) ([]byte, error) {
	return runBn254Add(input)
}

func (c *bn254AddByzantium) Name() string {
	return "BN254_ADD" // note bn254 is the correct name and is required by eth_config
}

// runBn254ScalarMul implements the Bn254ScalarMul precompile, referenced by
// both Byzantium and Istanbul operations.
func runBn254ScalarMul(input []byte) ([]byte, error) {
	x := bn254.G1Affine{}
	err := libbn254.UnmarshalCurvePointG1(getData(input, 0, 64), &x)
	if err != nil {
		return nil, err
	}
	return libbn254.MarshalCurvePointG1(x.ScalarMultiplication(&x, new(big.Int).SetBytes(getData(input, 64, 32)))), nil
}

// bn254ScalarMulIstanbul implements a native elliptic curve scalar
// multiplication conforming to Istanbul consensus rules.
type bn254ScalarMulIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn254ScalarMulIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn254ScalarMulGasIstanbul
}

func (c *bn254ScalarMulIstanbul) Run(input []byte) ([]byte, error) {
	return runBn254ScalarMul(input)
}

func (c *bn254ScalarMulIstanbul) Name() string {
	return "BN254_MUL" // note bn254 is the correct name and is required by eth_config
}

// bn254ScalarMulByzantium implements a native elliptic curve scalar
// multiplication conforming to Byzantium consensus rules.
type bn254ScalarMulByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn254ScalarMulByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn254ScalarMulGasByzantium
}

func (c *bn254ScalarMulByzantium) Run(input []byte) ([]byte, error) {
	return runBn254ScalarMul(input)
}

func (c *bn254ScalarMulByzantium) Name() string {
	return "BN254_MUL" // note bn254 is the correct name and is required by eth_config
}

var (
	// true32Byte is returned if the bn254 pairing check succeeds.
	true32Byte = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

	// false32Byte is returned if the bn254 pairing check fails.
	false32Byte = make([]byte, 32)

	// errBadPairingInput is returned if the bn254 pairing input is invalid.
	errBadPairingInput = errors.New("bad elliptic curve pairing size")
)

// runBn254Pairing implements the Bn254Pairing precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn254Pairing(input []byte) ([]byte, error) {
	// Handle some corner cases cheaply
	if len(input) == 0 {
		return true32Byte, nil
	}
	if len(input)%192 > 0 {
		return nil, errBadPairingInput
	}

	// Convert the input into a set of coordinates
	as := make([]bn254.G1Affine, 0, len(input)/192)
	bs := make([]bn254.G2Affine, 0, len(input)/192)
	for i := 0; i < len(input); i += 192 {
		ai := bn254.G1Affine{}
		err := libbn254.UnmarshalCurvePointG1(input[i:i+64], &ai)
		if err != nil {
			return nil, err
		}

		bi := bn254.G2Affine{}
		err = libbn254.UnmarshalCurvePointG2(input[i+64:i+192], &bi)
		if err != nil {
			return nil, err
		}
		as = append(as, ai)
		bs = append(bs, bi)
	}

	success, err := bn254.PairingCheck(as, bs)
	if err != nil {
		return nil, err
	}
	if success {
		return true32Byte, nil
	}
	return false32Byte, nil
}

// bn254PairingIstanbul implements a pairing pre-compile for the bn254 curve
// conforming to Istanbul consensus rules.
type bn254PairingIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn254PairingIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn254PairingBaseGasIstanbul + uint64(len(input)/192)*params.Bn254PairingPerPointGasIstanbul
}

func (c *bn254PairingIstanbul) Run(input []byte) ([]byte, error) {
	return runBn254Pairing(input)
}

func (c *bn254PairingIstanbul) Name() string {
	return "BN254_PAIRING" // note bn254 is the correct name and is required by eth_config
}

// bn254PairingByzantium implements a pairing pre-compile for the bn254 curve
// conforming to Byzantium consensus rules.
type bn254PairingByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn254PairingByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn254PairingBaseGasByzantium + uint64(len(input)/192)*params.Bn254PairingPerPointGasByzantium
}

func (c *bn254PairingByzantium) Run(input []byte) ([]byte, error) {
	return runBn254Pairing(input)
}

func (c *bn254PairingByzantium) Name() string {
	return "BN254_PAIRING" // note bn254 is the correct name and is required by eth_config
}

type blake2F struct{}

func (c *blake2F) RequiredGas(input []byte) uint64 {
	// If the input is malformed, we can't calculate the gas, return 0 and let the
	// actual call choke and fault.
	if len(input) != blake2FInputLength {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(input[0:4]))
}

const (
	blake2FInputLength        = 213
	blake2FFinalBlockBytes    = byte(1)
	blake2FNonFinalBlockBytes = byte(0)
)

var (
	errBlake2FInvalidInputLength = errors.New("invalid input length")
	errBlake2FInvalidFinalFlag   = errors.New("invalid final flag")
)

func (c *blake2F) Run(input []byte) ([]byte, error) {
	// Make sure the input is valid (correct length and final flag)
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

func (c *blake2F) Name() string {
	return "BLAKE2F"
}

var (
	errBLS12381InvalidInputLength          = errors.New("invalid input length")
	errBLS12381InvalidFieldElementTopBytes = errors.New("invalid field element top bytes")
	errBLS12381G1PointSubgroup             = errors.New("g1 point is not on correct subgroup")
	errBLS12381G2PointSubgroup             = errors.New("g2 point is not on correct subgroup")
)

// bls12381G1Add implements EIP-2537 G1Add precompile.
type bls12381G1Add struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1Add) RequiredGas(input []byte) uint64 {
	return params.Bls12381G1AddGas
}

func (c *bls12381G1Add) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1Add precompile.
	// > G1 addition call expects `256` bytes as an input that is interpreted as byte concatenation of two G1 points (`128` bytes each).
	// > Output is an encoding of addition operation result - single G1 point (`128` bytes).
	if len(input) != 256 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0, p1 *bls12381.G1Affine

	// Decode G1 point p_0
	if p0, err = decodePointG1(input[:128]); err != nil {
		return nil, err
	}
	// Decode G1 point p_1
	if p1, err = decodePointG1(input[128:]); err != nil {
		return nil, err
	}

	// Compute r = p_0 + p_1
	p0.Add(p0, p1)

	// Encode the G1 point result into 128 bytes
	return encodePointG1(p0), nil
}

func (c *bls12381G1Add) Name() string {
	return "BLS12_G1ADD"
}

// bls12381G1MultiExp implements EIP-2537 G1MultiExp precompile.
type bls12381G1MultiExp struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G1MultiExp) RequiredGas(input []byte) uint64 {
	// Calculate G1 point, scalar value pair length
	k := len(input) / 160
	if k == 0 {
		// Return 0 gas for small input length
		return 0
	}
	// Lookup discount value for G1 point, scalar value pair length
	var discount uint64
	if dLen := len(params.Bls12381MSMDiscountTableG1); k < dLen {
		discount = params.Bls12381MSMDiscountTableG1[k-1]
	} else {
		discount = params.Bls12381MSMDiscountTableG1[dLen-1]
	}
	// Calculate gas and return the result
	return (uint64(k) * params.Bls12381G1MulGas * discount) / 1000
}

func (c *bls12381G1MultiExp) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G1MultiExp precompile.
	// G1 multiplication call expects `160*k` bytes as an input that is interpreted as byte concatenation of `k` slices each of them being a byte concatenation of encoding of G1 point (`128` bytes) and encoding of a scalar value (`32` bytes).
	// Output is an encoding of multiexponentiation operation result - single G1 point (`128` bytes).
	k := len(input) / 160
	if len(input) == 0 || len(input)%160 != 0 {
		return nil, errBLS12381InvalidInputLength
	}
	points := make([]bls12381.G1Affine, k)
	scalars := make([]fr.Element, k)

	// Decode point scalar pairs
	for i := 0; i < k; i++ {
		off := 160 * i
		t0, t1, t2 := off, off+128, off+160
		// Decode G1 point
		p, err := decodePointG1(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Fast subgroup check
		if !p.IsInSubGroup() {
			return nil, errBLS12381G1PointSubgroup
		}
		points[i] = *p
		// Decode scalar value
		scalars[i] = *new(fr.Element).SetBytes(input[t1:t2])
	}

	// Compute r = e_0 * p_0 + e_1 * p_1 + ... + e_(k-1) * p_(k-1)
	r := new(bls12381.G1Affine)
	r.MultiExp(points, scalars, ecc.MultiExpConfig{})

	// Encode the G1 point to 128 bytes
	return encodePointG1(r), nil
}

func (c *bls12381G1MultiExp) Name() string {
	return "BLS12_G1MSM"
}

// bls12381G2Add implements EIP-2537 G2Add precompile.
type bls12381G2Add struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2Add) RequiredGas(input []byte) uint64 {
	return params.Bls12381G2AddGas
}

func (c *bls12381G2Add) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2Add precompile.
	// > G2 addition call expects `512` bytes as an input that is interpreted as byte concatenation of two G2 points (`256` bytes each).
	// > Output is an encoding of addition operation result - single G2 point (`256` bytes).
	if len(input) != 512 {
		return nil, errBLS12381InvalidInputLength
	}
	var err error
	var p0, p1 *bls12381.G2Affine

	// Decode G2 point p_0
	if p0, err = decodePointG2(input[:256]); err != nil {
		return nil, err
	}
	// Decode G2 point p_1
	if p1, err = decodePointG2(input[256:]); err != nil {
		return nil, err
	}

	// Compute r = p_0 + p_1
	r := new(bls12381.G2Affine)
	r.Add(p0, p1)

	// Encode the G2 point into 256 bytes
	return encodePointG2(r), nil
}

func (c *bls12381G2Add) Name() string {
	return "BLS12_G2ADD"
}

// bls12381G2MultiExp implements EIP-2537 G2MultiExp precompile.
type bls12381G2MultiExp struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381G2MultiExp) RequiredGas(input []byte) uint64 {
	// Calculate G2 point, scalar value pair length
	k := len(input) / 288
	if k == 0 {
		// Return 0 gas for small input length
		return 0
	}
	// Lookup discount value for G2 point, scalar value pair length
	var discount uint64
	if dLen := len(params.Bls12381MSMDiscountTableG2); k < dLen {
		discount = params.Bls12381MSMDiscountTableG2[k-1]
	} else {
		discount = params.Bls12381MSMDiscountTableG2[dLen-1]
	}
	// Calculate gas and return the result
	return (uint64(k) * params.Bls12381G2MulGas * discount) / 1000
}

func (c *bls12381G2MultiExp) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 G2MultiExp precompile logic
	// > G2 multiplication call expects `288*k` bytes as an input that is interpreted as byte concatenation of `k` slices each of them being a byte concatenation of encoding of G2 point (`256` bytes) and encoding of a scalar value (`32` bytes).
	// > Output is an encoding of multiexponentiation operation result - single G2 point (`256` bytes).
	k := len(input) / 288
	if len(input) == 0 || len(input)%288 != 0 {
		return nil, errBLS12381InvalidInputLength
	}
	points := make([]bls12381.G2Affine, k)
	scalars := make([]fr.Element, k)

	// Decode point scalar pairs
	for i := 0; i < k; i++ {
		off := 288 * i
		t0, t1, t2 := off, off+256, off+288
		// Decode G2 point
		p, err := decodePointG2(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Fast subgroup check
		if !p.IsInSubGroup() {
			return nil, errBLS12381G2PointSubgroup
		}
		points[i] = *p
		// Decode scalar value
		scalars[i] = *new(fr.Element).SetBytes(input[t1:t2])
	}

	// Compute r = e_0 * p_0 + e_1 * p_1 + ... + e_(k-1) * p_(k-1)
	r := new(bls12381.G2Affine)
	r.MultiExp(points, scalars, ecc.MultiExpConfig{})

	// Encode the G2 point to 256 bytes.
	return encodePointG2(r), nil
}

func (c *bls12381G2MultiExp) Name() string {
	return "BLS12_G2MSM"
}

// bls12381Pairing implements EIP-2537 Pairing precompile.
type bls12381Pairing struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381Pairing) RequiredGas(input []byte) uint64 {
	return params.Bls12381PairingBaseGas + uint64(len(input)/384)*params.Bls12381PairingPerPairGas
}

func (c *bls12381Pairing) Run(input []byte) ([]byte, error) {
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

	var (
		p []bls12381.G1Affine
		q []bls12381.G2Affine
	)

	// Decode pairs
	for i := 0; i < k; i++ {
		off := 384 * i
		t0, t1, t2 := off, off+128, off+384

		// Decode G1 point
		p1, err := decodePointG1(input[t0:t1])
		if err != nil {
			return nil, err
		}
		// Decode G2 point
		p2, err := decodePointG2(input[t1:t2])
		if err != nil {
			return nil, err
		}

		// 'point is on curve' check already done,
		// Here we need to apply subgroup checks.
		if !p1.IsInSubGroup() {
			return nil, errBLS12381G1PointSubgroup
		}
		if !p2.IsInSubGroup() {
			return nil, errBLS12381G2PointSubgroup
		}
		p = append(p, *p1)
		q = append(q, *p2)
	}
	// Prepare 32 byte output
	out := make([]byte, 32)

	// Compute pairing and set the result
	ok, err := bls12381.PairingCheck(p, q)
	if err == nil && ok {
		out[31] = 1
	}
	return out, nil
}

func (c *bls12381Pairing) Name() string {
	return "BLS12_PAIRING_CHECK"
}

func decodePointG1(in []byte) (*bls12381.G1Affine, error) {
	if len(in) != 128 {
		return nil, errors.New("invalid g1 point length")
	}
	// decode x
	x, err := decodeBLS12381FieldElement(in[:64])
	if err != nil {
		return nil, err
	}
	// decode y
	y, err := decodeBLS12381FieldElement(in[64:])
	if err != nil {
		return nil, err
	}
	elem := bls12381.G1Affine{X: x, Y: y}
	if !elem.IsOnCurve() {
		return nil, errors.New("invalid point: not on curve")
	}

	return &elem, nil
}

// decodePointG2 given encoded (x, y) coordinates in 256 bytes returns a valid G2 Point.
func decodePointG2(in []byte) (*bls12381.G2Affine, error) {
	if len(in) != 256 {
		return nil, errors.New("invalid g2 point length")
	}
	x0, err := decodeBLS12381FieldElement(in[:64])
	if err != nil {
		return nil, err
	}
	x1, err := decodeBLS12381FieldElement(in[64:128])
	if err != nil {
		return nil, err
	}
	y0, err := decodeBLS12381FieldElement(in[128:192])
	if err != nil {
		return nil, err
	}
	y1, err := decodeBLS12381FieldElement(in[192:])
	if err != nil {
		return nil, err
	}

	p := bls12381.G2Affine{X: bls12381.E2{A0: x0, A1: x1}, Y: bls12381.E2{A0: y0, A1: y1}}
	if !p.IsOnCurve() {
		return nil, errors.New("invalid point: not on curve")
	}
	return &p, err
}

// decodeBLS12381FieldElement decodes BLS12-381 elliptic curve field element.
// Removes top 16 bytes of 64 byte input.
func decodeBLS12381FieldElement(in []byte) (fp.Element, error) {
	if len(in) != 64 {
		return fp.Element{}, errors.New("invalid field element length")
	}
	// check top bytes
	for i := 0; i < 16; i++ {
		if in[i] != byte(0x00) {
			return fp.Element{}, errBLS12381InvalidFieldElementTopBytes
		}
	}
	var res [48]byte
	copy(res[:], in[16:])

	return fp.BigEndian.Element(&res)
}

// encodePointG1 encodes a point into 128 bytes.
func encodePointG1(p *bls12381.G1Affine) []byte {
	out := make([]byte, 128)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[16:]), p.X)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[64+16:]), p.Y)
	return out
}

// encodePointG2 encodes a point into 256 bytes.
func encodePointG2(p *bls12381.G2Affine) []byte {
	out := make([]byte, 256)
	// encode x
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[16:16+48]), p.X.A0)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[80:80+48]), p.X.A1)
	// encode y
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[144:144+48]), p.Y.A0)
	fp.BigEndian.PutElement((*[fp.Bytes]byte)(out[208:208+48]), p.Y.A1)
	return out
}

// bls12381MapFpToG1 implements EIP-2537 MapG1 precompile.
type bls12381MapFpToG1 struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381MapFpToG1) RequiredGas(input []byte) uint64 {
	return params.Bls12381MapFpToG1Gas
}

func (c *bls12381MapFpToG1) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Map_To_G1 precompile.
	// > Field-to-curve call expects `64` bytes as an input that is interpreted as a an element of the base field.
	// > Output of this call is `128` bytes and is G1 point following respective encoding rules.
	if len(input) != 64 {
		return nil, errBLS12381InvalidInputLength
	}

	// Decode input field element
	fe, err := decodeBLS12381FieldElement(input)
	if err != nil {
		return nil, err
	}

	// Compute mapping
	r := bls12381.MapToG1(fe)

	// Encode the G1 point to 128 bytes
	return encodePointG1(&r), nil
}

func (c *bls12381MapFpToG1) Name() string {
	return "BLS12_MAP_FP_TO_G1"
}

// bls12381MapFp2ToG2 implements EIP-2537 MapG2 precompile.
type bls12381MapFp2ToG2 struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bls12381MapFp2ToG2) RequiredGas(input []byte) uint64 {
	return params.Bls12381MapFp2ToG2Gas
}

func (c *bls12381MapFp2ToG2) Run(input []byte) ([]byte, error) {
	// Implements EIP-2537 Map_FP2_TO_G2 precompile logic.
	// > Field-to-curve call expects `128` bytes as an input that is interpreted as a an element of the quadratic extension field.
	// > Output of this call is `256` bytes and is G2 point following respective encoding rules.
	if len(input) != 128 {
		return nil, errBLS12381InvalidInputLength
	}

	// Decode input field element
	c0, err := decodeBLS12381FieldElement(input[:64])
	if err != nil {
		return nil, err
	}
	c1, err := decodeBLS12381FieldElement(input[64:])
	if err != nil {
		return nil, err
	}

	// Compute mapping
	r := bls12381.MapToG2(bls12381.E2{A0: c0, A1: c1})

	// Encode the G2 point to 256 bytes
	return encodePointG2(&r), nil
}

func (c *bls12381MapFp2ToG2) Name() string {
	return "BLS12_MAP_FP2_TO_G2"
}

// pointEvaluation implements the EIP-4844 point evaluation precompile
// to check if a value is part of a blob at a specific point with a KZG proof.
type pointEvaluation struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *pointEvaluation) RequiredGas(input []byte) uint64 {
	return params.PointEvaluationGas
}

func (c *pointEvaluation) Run(input []byte) ([]byte, error) {
	return libkzg.PointEvaluationPrecompile(input)
}

func (c *pointEvaluation) Name() string {
	return "KZG_POINT_EVALUATION"
}

// P256VERIFY (secp256r1 signature verification)
// implemented as a native contract
type p256Verify struct {
	eip7951 bool
}

// RequiredGas returns the gas required to execute the precompiled contract
func (c *p256Verify) RequiredGas(input []byte) uint64 {
	if c.eip7951 {
		return params.P256VerifyGasEIP7951
	}
	return params.P256VerifyGas
}

// Run executes the precompiled contract with given 160 bytes of param, returning the output and the used gas
func (c *p256Verify) Run(input []byte) ([]byte, error) {
	// Required input length is 160 bytes
	const p256VerifyInputLength = 160
	// Check the input length
	if len(input) != p256VerifyInputLength {
		// Input length is invalid
		return nil, nil
	}

	// Extract the hash, r, s, x, y from the input
	hash := input[0:32]
	r, s := new(big.Int).SetBytes(input[32:64]), new(big.Int).SetBytes(input[64:96])
	x, y := new(big.Int).SetBytes(input[96:128]), new(big.Int).SetBytes(input[128:160])

	// Verify the secp256r1 signature
	if secp256r1.Verify(hash, r, s, x, y) {
		// Signature is valid
		return common.LeftPadBytes(common.Big1.Bytes(), 32), nil
	} else {
		// Signature is invalid
		return nil, nil
	}
}

func (c *p256Verify) Name() string {
	return "P256VERIFY"
}
