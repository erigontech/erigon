// Copyright 2016 The go-ethereum Authors
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

package types

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/params"
)

var ErrInvalidChainId = errors.New("invalid chain id for signer")

// MakeSigner returns a Signer based on the given chain config and block number.
func MakeSigner(config *params.ChainConfig, blockNumber *big.Int) Signer {
	var signer Signer
	signer.unprotected = true
	switch {
	case config.IsAleut(blockNumber):
		// All transaction types are still supported
		signer.protected = true
		signer.accesslist = true
		signer.dynamicfee = true
	case config.IsBerlin(blockNumber):
		signer.protected = true
		signer.accesslist = true
	case config.IsEIP155(blockNumber):
		signer.protected = true
	case config.IsHomestead(blockNumber):
	default:
		// Only allow malleable transactions in Frontier
		signer.maleable = true
	}
	return signer
}

// LatestSigner returns the 'most permissive' Signer available for the given chain
// configuration. Specifically, this enables support of EIP-155 replay protection and
// EIP-2930 access list transactions when their respective forks are scheduled to occur at
// any block number in the chain config.
//
// Use this in transaction-handling code where the current block number is unknown. If you
// have the current block number available, use MakeSigner instead.
func LatestSigner(config *params.ChainConfig) *Signer {
	var signer Signer
	signer.unprotected = true
	if config.ChainID != nil {
		if config.BerlinBlock != nil || config.YoloV3Block != nil {
			signer.protected = true
			signer.accesslist = true
			signer.dynamicfee = true
		}
		if config.EIP155Block != nil {
			signer.protected = true
		}
	}
	return &signer
}

// LatestSignerForChainID returns the 'most permissive' Signer available. Specifically,
// this enables support for EIP-155 replay protection and all implemented EIP-2718
// transaction types if chainID is non-nil.
//
// Use this in transaction-handling code where the current block number and fork
// configuration are unknown. If you have a ChainConfig, use LatestSigner instead.
// If you have a ChainConfig and know the current block number, use MakeSigner instead.
func LatestSignerForChainID(chainID *big.Int) Signer {
	if chainID == nil {
		return HomesteadSigner{}
	}
	return NewEIP2930Signer(chainID)
}

// SignTx signs the transaction using the given signer and private key.
func SignTx(tx Transaction, s Signer, prv *ecdsa.PrivateKey) (Transaction, error) {
	h := s.Hash(tx)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

// SignNewTx creates a transaction and signs it.
func SignNewTx(prv *ecdsa.PrivateKey, s Signer, tx Transaction) (Transaction, error) {
	tx := NewTx(txdata)
	h := s.Hash(tx)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

// MustSignNewTx creates a transaction and signs it.
// This panics if the transaction cannot be signed.
func MustSignNewTx(prv *ecdsa.PrivateKey, s Signer, txdata TxData) Transaction {
	tx, err := SignNewTx(prv, s, txdata)
	if err != nil {
		panic(err)
	}
	return tx
}

// Sender returns the address derived from the signature (V, R, S) using secp256k1
// elliptic curve and an error if it failed deriving or upon an incorrect
// signature.
//
// Sender may cache the address, allowing it to be used regardless of
// signing method. The cache is invalidated if the cached signer does
// not match the signer used in the current call.
func Sender(signer Signer, tx Transaction) (common.Address, error) {
	if sc := tx.From().Load(); sc != nil {
		return sc.(common.Address), nil
	}
	addr, err := signer.Sender(tx)
	if err != nil {
		return common.Address{}, err
	}
	tx.From().Store(addr)
	return addr, nil
}

// Signer encapsulates transaction signature handling. The name of this type is slightly
// misleading because Signers don't actually sign, they're just for validating and
// processing of signatures.
//
// Note that this interface is not a stable API and may change at any time to accommodate
// new protocol rules.
type Signer struct {
	chainID, chainIDMul uint256.Int
	maleable            bool // Whether this signer should allow legacy transactions with malleability
	unprotected         bool // Whether this signer should allow legacy transactions without chainId protection
	protected           bool // Whether this signer should allow transactions with replay protection via chainId
	accesslist          bool // Whether this signer should allow transactions with access list, superseeds protected
	dynamicfee          bool // Whether this signer should allow transactions with basefee and tip (instead of gasprice), superseeds accesslist
}

func (sg Signer) String() string {
	return fmt.Sprintf("Signer[chainId=%d,unprotected=%t,protected=%t,accesslist=%t,basefee=%d")
}

// Sender returns the sender address of the transaction.
func (sg Signer) Sender(tx Transaction) (common.Address, error) {
	return sg.SenderWithContext(secp256k1.DefaultContext, tx)
}

// SenderWithContext returns the sender address of the transaction.
func (sg Signer) SenderWithContext(context *secp256k1.Context, tx Transaction) (common.Address, error) {
	var V uint256.Int
	var R, S *uint256.Int
	var hash common.Hash
	switch t := tx.(type) {
	case *LegacyTx:
		if !t.Protected() {
			if !sg.unprotected {
				return common.Address{}, fmt.Errorf("unprotected tx is not supported by signer %s", sg)
			}
			V.Set(t.V)
			hash = rlpHash([]interface{}{
				t.Nonce,
				t.GasPrice,
				t.Gas,
				t.To,
				t.Value,
				t.Data,
			})
		} else {
			if !sg.protected {
				return common.Address{}, fmt.Errorf("protected tx is not supported by signer %s", sg)
			}
			if !deriveChainId(t.V).Eq(&sg.chainID) {
				return common.Address{}, ErrInvalidChainId
			}
			V.Sub(t.V, &sg.chainIDMul)
			V.Sub(&V, u256.Num8)
			hash = rlpHash([]interface{}{
				t.Nonce,
				t.GasPrice,
				t.Gas,
				t.To,
				t.Value,
				t.Data,
				sg.chainID.ToBig(), uint(0), uint(0),
			})
		}
		R, S = t.R, t.S
	case *AccessListTx:
		if !sg.accesslist {
			return common.Address{}, fmt.Errorf("accesslist tx is not supported by signer %s", sg)
		}
		if !t.ChainID.Eq(&sg.chainID) {
			return common.Address{}, ErrInvalidChainId
		}
		// ACL txs are defined to use 0 and 1 as their recovery id, add
		// 27 to become equivalent to unprotected Homestead signatures.
		V.Add(t.V, u256.Num27)
		R, S = t.R, t.S
		hash = prefixedRlpHash(
			AccessListTxType,
			[]interface{}{
				t.ChainID.ToBig(),
				t.Nonce,
				t.GasPrice,
				t.Gas,
				t.To,
				t.Value,
				t.Data,
				t.AccessList,
			})
	case *DynamicFeeTransaction:
		if !sg.dynamicfee {
			return common.Address{}, fmt.Errorf("dynamicfee tx is not supported by signer %s", sg)
		}
		if !t.ChainID.Eq(&sg.chainID) {
			return common.Address{}, ErrInvalidChainId
		}
		// ACL and DynamicFee txs are defined to use 0 and 1 as their recovery
		// id, add 27 to become equivalent to unprotected Homestead signatures.
		V.Add(t.V, u256.Num27)
		R, S = t.R, t.S
		hash = prefixedRlpHash(
			DynamicFeeTxType,
			[]interface{}{
				t.ChainID,
				t.Nonce,
				t.Tip,
				t.FeeCap,
				t.Gas,
				t.To,
				t.Value,
				t.Data,
				t.AccessList,
			})
	default:
		return common.Address{}, ErrTxTypeNotSupported
	}
	return recoverPlain(context, hash, R, S, &V, !sg.maleable)
}

// SignatureValues returns the raw R, S, V values corresponding to the
// given signature.
func (sg Signer) SignatureValues(tx Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	switch t := tx.(type) {
	case *LegacyTx:
		R, S, V = decodeSignature(sig)
		if sg.chainID.IsZero() {
			V.Add(V, u256.Num27)
		} else {
			V.Add(V, u256.Num35)
			V.Add(V, &sg.chainIDMul)
		}
	case *AccessListTx:
		// Check that chain ID of tx matches the signer. We also accept ID zero here,
		// because it indicates that the chain ID was not specified in the tx.
		if !t.ChainID.IsZero() && !t.ChainID.Eq(&sg.chainID) {
			return nil, nil, nil, ErrInvalidChainId
		}
		R, S, V = decodeSignature(sig)
	case *DynamicFeeTransaction:
		// Check that chain ID of tx matches the signer. We also accept ID zero here,
		// because it indicates that the chain ID was not specified in the tx.
		if !t.ChainID.IsZero() && !t.ChainID.Eq(&sg.chainID) {
			return nil, nil, nil, ErrInvalidChainId
		}
		R, S, V = decodeSignature(sig)
	default:
		return nil, nil, nil, ErrTxTypeNotSupported
	}
	return R, S, V, nil
}

func (sg Signer) ChainID() *uint256.Int {
	return &sg.chainID
}

// Hash returns 'signature hash', i.e. the transaction hash that is signed by the
// private key. This hash does not uniquely identify the transaction.
func (sg Signer) Hash(tx Transaction) common.Hash {
	return common.Hash{}
}

// Equal returns true if the given signer is the same as the receiver.
func (sg Signer) Equal(other Signer) bool {
	return sg.chainID.Eq(&other.chainID) &&
		sg.maleable == sg.maleable &&
		sg.unprotected == sg.unprotected &&
		sg.protected == sg.protected &&
		sg.accesslist == sg.accesslist &&
		sg.dynamicfee == sg.dynamicfee
}

type eip2930Signer struct{ EIP155Signer }

// NewEIP2930Signer returns a signer that accepts EIP-2930 access list transactions,
// EIP-155 replay protected transactions, and legacy Homestead transactions.
func NewEIP2930Signer(chainId *big.Int) eip2930Signer {
	return eip2930Signer{NewEIP155Signer(chainId)}
}

func (s eip2930Signer) ChainID() *uint256.Int {
	return s.chainID
}

func (s eip2930Signer) Equal(s2 Signer) bool {
	x, ok := s2.(eip2930Signer)
	return ok && x.chainID.Cmp(s.chainID) == 0
}

func (s eip2930Signer) Sender(tx Transaction) (common.Address, error) {
	return s.SenderWithContext(secp256k1.DefaultContext, tx)
}

func (s eip2930Signer) SenderWithContext(context *secp256k1.Context, tx Transaction) (common.Address, error) {
	var V uint256.Int
	var R, S *uint256.Int
	switch t := tx.(type) {
	case *LegacyTx:
		if !t.Protected() {
			return HomesteadSigner{}.Sender(t)
		}
		V.Sub(t.V, s.chainIDMul)
		V.Sub(&V, u256.Num8)
		R, S = t.R, t.S
	case *AccessListTx:
		// ACL txs are defined to use 0 and 1 as their recovery id, add
		// 27 to become equivalent to unprotected Homestead signatures.
		V.Add(t.V, u256.Num27)
		R, S = t.R, t.S
	case *DynamicFeeTransaction:
		// ACL and DynamicFee txs are defined to use 0 and 1 as their recovery
		// id, add 27 to become equivalent to unprotected Homestead signatures.
		V.Add(t.V, u256.Num27)
		R, S = t.R, t.S
	default:
		return common.Address{}, ErrTxTypeNotSupported
	}
	if tx.ChainId().Cmp(s.chainID) != 0 {
		return common.Address{}, ErrInvalidChainId
	}
	return recoverPlain(context, s.Hash(tx), R, S, &V, true)
}

func (s eip2930Signer) SignatureValues(tx Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	switch txdata := tx.(type) {
	case *LegacyTx:
		R, S, V = decodeSignature(sig)
		if s.chainID.Sign() != 0 {
			V = uint256.NewInt().SetUint64(uint64(sig[64] + 35))
			V.Add(V, s.chainIDMul)
		}
	case *AccessListTx:
		// Check that chain ID of tx matches the signer. We also accept ID zero here,
		// because it indicates that the chain ID was not specified in the tx.
		if txdata.ChainID.Sign() != 0 && txdata.ChainID.Cmp(s.chainID) != 0 {
			return nil, nil, nil, ErrInvalidChainId
		}
		R, S, _ = decodeSignature(sig)
		V = uint256.NewInt().SetUint64(uint64(sig[64]))
	case *DynamicFeeTransaction:
		// Check that chain ID of tx matches the signer. We also accept ID zero here,
		// because it indicates that the chain ID was not specified in the tx.
		if txdata.ChainID.Sign() != 0 && txdata.ChainID.Cmp(s.chainID) != 0 {
			return nil, nil, nil, ErrInvalidChainId
		}
		R, S, _ = decodeSignature(sig)
		V = uint256.NewInt().SetUint64(uint64(sig[64]))
	default:
		return nil, nil, nil, ErrTxTypeNotSupported
	}
	return R, S, V, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (s eip2930Signer) Hash(tx Transaction) common.Hash {
	switch tx.Type() {
	case LegacyTxType:
		return rlpHash([]interface{}{
			tx.Nonce(),
			tx.GasPrice(),
			tx.Gas(),
			tx.To(),
			tx.Value(),
			tx.Data(),
			s.chainID, uint(0), uint(0),
		})
	case AccessListTxType:
		return prefixedRlpHash(
			tx.Type(),
			[]interface{}{
				s.chainID,
				tx.Nonce(),
				tx.GasPrice(),
				tx.Gas(),
				tx.To(),
				tx.Value(),
				tx.Data(),
				tx.AccessList(),
			})
	default:
		// This _should_ not happen, but in case someone sends in a bad
		// json struct via RPC, it's probably more prudent to return an
		// empty hash instead of killing the node with a panic
		//panic("Unsupported transaction type: %d", tx.typ)
		return common.Hash{}
	}
}

// EIP155Signer implements Signer using the EIP-155 rules. This accepts transactions which
// are replay-protected as well as unprotected homestead transactions.
type EIP155Signer struct {
	chainID, chainIDMul *uint256.Int
}

func NewEIP155Signer(chainId *big.Int) EIP155Signer {
	x := uint256.NewInt()
	if chainId != nil {
		x.SetFromBig(chainId)
	}
	return EIP155Signer{
		chainID:    x,
		chainIDMul: new(uint256.Int).Mul(x, u256.Num2),
	}
}

func (s EIP155Signer) ChainID() *uint256.Int {
	return s.chainID
}

func (s EIP155Signer) Equal(s2 Signer) bool {
	eip155, ok := s2.(EIP155Signer)
	return ok && eip155.chainID.Cmp(s.chainID) == 0
}

func (s EIP155Signer) Sender(tx Transaction) (common.Address, error) {
	return s.SenderWithContext(secp256k1.DefaultContext, tx)
}

func (s EIP155Signer) SenderWithContext(context *secp256k1.Context, tx Transaction) (common.Address, error) {
	t, ok := tx.(*LegacyTx)
	if !ok {
		return common.Address{}, ErrTxTypeNotSupported
	}
	if !t.Protected() {
		return HomesteadSigner{}.Sender(tx)
	}
	if !deriveChainId(t.V).Eq(s.chainID) {
		return common.Address{}, ErrInvalidChainId
	}
	V := new(uint256.Int).Sub(t.V, s.chainIDMul) // It needs to be new(uint256) to make sure we don't modify the tx's value
	V.Sub(V, u256.Num8)
	return recoverPlain(context, s.Hash(tx), t.R, t.S, V, true)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (s EIP155Signer) SignatureValues(tx Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	if tx.Type() != LegacyTxType {
		return nil, nil, nil, ErrTxTypeNotSupported
	}
	R, S, V = decodeSignature(sig)
	if s.chainID.Sign() != 0 {
		V = uint256.NewInt().SetUint64(uint64(sig[64] + 35))
		V.Add(V, s.chainIDMul)
	}
	return R, S, V, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (s EIP155Signer) Hash(tx Transaction) common.Hash {
	return rlpHash([]interface{}{
		tx.Nonce(),
		tx.GasPrice(),
		tx.Gas(),
		tx.To(),
		tx.Value(),
		tx.Data(),
		s.chainID, uint(0), uint(0),
	})
}

// HomesteadTransaction implements TransactionInterface using the
// homestead rules.
type HomesteadSigner struct{ FrontierSigner }

func (hs HomesteadSigner) ChainID() *uint256.Int {
	return nil
}

func (hs HomesteadSigner) Equal(s2 Signer) bool {
	_, ok := s2.(HomesteadSigner)
	return ok
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (hs HomesteadSigner) SignatureValues(tx Transaction, sig []byte) (r, s, v *uint256.Int, err error) {
	return hs.FrontierSigner.SignatureValues(tx, sig)
}

func (hs HomesteadSigner) Sender(tx Transaction) (common.Address, error) {
	if tx.Type() != LegacyTxType {
		return common.Address{}, ErrTxTypeNotSupported
	}
	return hs.SenderWithContext(secp256k1.DefaultContext, tx)
}

func (hs HomesteadSigner) SenderWithContext(context *secp256k1.Context, tx Transaction) (common.Address, error) {
	v, r, s := tx.RawSignatureValues()
	return recoverPlain(context, hs.Hash(tx), r, s, v, true)
}

type FrontierSigner struct{}

func (fs FrontierSigner) Equal(s2 Signer) bool {
	_, ok := s2.(FrontierSigner)
	return ok
}

func (fs FrontierSigner) Sender(tx Transaction) (common.Address, error) {
	if tx.Type() != LegacyTxType {
		return common.Address{}, ErrTxTypeNotSupported
	}
	v, r, s := tx.RawSignatureValues()
	return recoverPlain(secp256k1.DefaultContext, fs.Hash(tx), r, s, v, false)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (fs FrontierSigner) SignatureValues(tx Transaction, sig []byte) (r, s, v *uint256.Int, err error) {
	if tx.Type() != LegacyTxType {
		return nil, nil, nil, ErrTxTypeNotSupported
	}
	r, s, v = decodeSignature(sig)
	return r, s, v, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (fs FrontierSigner) Hash(tx Transaction) common.Hash {
	return rlpHash([]interface{}{
		tx.Nonce(),
		tx.GasPrice(),
		tx.Gas(),
		tx.To(),
		tx.Value(),
		tx.Data(),
	})
}

func decodeSignature(sig []byte) (r, s, v *uint256.Int) {
	if len(sig) != crypto.SignatureLength {
		panic(fmt.Sprintf("wrong size for signature: got %d, want %d", len(sig), crypto.SignatureLength))
	}
	r = new(uint256.Int).SetBytes(sig[:32])
	s = new(uint256.Int).SetBytes(sig[32:64])
	v = new(uint256.Int).SetBytes(sig[64:65])
	return r, s, v
}

func (fs FrontierSigner) SenderWithContext(context *secp256k1.Context, tx Transaction) (common.Address, error) {
	v, r, s := tx.RawSignatureValues()
	return recoverPlain(context, fs.Hash(tx), r, s, v, false)
}

func (fs FrontierSigner) ChainID() *uint256.Int {
	return u256.Num0
}

func recoverPlain(context *secp256k1.Context, sighash common.Hash, R, S, Vb *uint256.Int, homestead bool) (common.Address, error) {
	if Vb.BitLen() > 8 {
		return common.Address{}, ErrInvalidSig
	}
	V := byte(Vb.Uint64() - 27)
	if !crypto.ValidateSignatureValues(V, R, S, homestead) {
		return common.Address{}, ErrInvalidSig
	}
	// encode the signature in uncompressed format
	r, s := R.Bytes(), S.Bytes()
	sig := make([]byte, crypto.SignatureLength)
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)
	sig[64] = V
	// recover the public key from the signature
	pub, err := crypto.EcrecoverWithContext(context, sighash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	if len(pub) == 0 || pub[0] != 4 {
		return common.Address{}, errors.New("invalid public key")
	}
	var addr common.Address
	copy(addr[:], crypto.Keccak256(pub[1:])[12:])
	return addr, nil
}

// deriveChainID derives the chain id from the given v parameter
func deriveChainId(v *uint256.Int) *uint256.Int {
	if v.IsUint64() {
		v := v.Uint64()
		if v == 27 || v == 28 {
			return new(uint256.Int)
		}
		return new(uint256.Int).SetUint64((v - 35) / 2)
	}
	v = new(uint256.Int).Sub(v, u256.Num35)
	return v.Div(v, u256.Num2)
}
