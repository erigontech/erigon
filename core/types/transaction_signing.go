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

var (
	ErrInvalidChainId = errors.New("invalid chain id for signer")
)

// MakeSigner returns a Signer based on the given chain config and block number.
func MakeSigner(config *params.ChainConfig, blockNumber *big.Int) Signer {
	var signer Signer
	switch {
	case config.IsEIP155(blockNumber):
		signer = NewEIP155Signer(config.ChainID)
	case config.IsHomestead(blockNumber):
		signer = HomesteadSigner{}
	default:
		signer = FrontierSigner{}
	}
	return signer
}

// SignTx signs the transaction using the given signer and private key
func SignTx(tx *Transaction, s Signer, prv *ecdsa.PrivateKey) (*Transaction, error) {
	h := s.Hash(tx)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

// Sender returns the address derived from the signature (V, R, S) using secp256k1
// elliptic curve and an error if it failed deriving or upon an incorrect
// signature.
//
// Sender may cache the address, allowing it to be used regardless of
// signing method. The cache is invalidated if the cached signer does
// not match the signer used in the current call.
func Sender(signer Signer, tx *Transaction) (common.Address, error) {
	if sc := tx.from.Load(); sc != nil {
		return sc.(common.Address), nil
	}
	addr, err := signer.Sender(tx)
	if err != nil {
		return common.Address{}, err
	}
	tx.from.Store(addr)
	return addr, nil
}

// Signer encapsulates transaction signature handling. Note that this interface is not a
// stable API and may change at any time to accommodate new protocol rules.
type Signer interface {
	// Sender returns the sender address of the transaction.
	Sender(tx *Transaction) (common.Address, error)
	// SenderWithContext returns the sender address of the transaction.
	SenderWithContext(context *secp256k1.Context, tx *Transaction) (common.Address, error)

	// SignatureValues returns the raw R, S, V values corresponding to the
	// given signature.
	SignatureValues(tx *Transaction, sig []byte) (r, s, v *uint256.Int, err error)
	// Hash returns the hash to be signed.
	Hash(tx *Transaction) common.Hash
	// Equal returns true if the given signer is the same as the receiver.
	Equal(Signer) bool
	// Return 0 for pre-EIP155 signers
	ChainID() *uint256.Int
}

// EIP155Transaction implements Signer using the EIP155 rules.
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

func (s EIP155Signer) Equal(s2 Signer) bool {
	eip155, ok := s2.(EIP155Signer)
	return ok && eip155.chainID.Cmp(s.chainID) == 0
}

func (s EIP155Signer) Sender(tx *Transaction) (common.Address, error) {
	return s.SenderWithContext(secp256k1.DefaultContext, tx)
}

func (s EIP155Signer) SenderWithContext(context *secp256k1.Context, tx *Transaction) (common.Address, error) {
	if !tx.Protected() {
		return HomesteadSigner{}.Sender(tx)
	}
	if !tx.ChainID().Eq(s.chainID) {
		return common.Address{}, ErrInvalidChainId
	}
	V := new(uint256.Int).Sub(&tx.data.V, s.chainIDMul)
	V.Sub(V, u256.Num8)
	return recoverPlain(context, s.Hash(tx), &tx.data.R, &tx.data.S, V, true)
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (s EIP155Signer) SignatureValues(tx *Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	R, S, V, err = HomesteadSigner{}.SignatureValues(tx, sig)
	if err != nil {
		return nil, nil, nil, err
	}
	if s.chainID.Sign() != 0 {
		V = uint256.NewInt().SetUint64(uint64(sig[64] + 35))
		V.Add(V, s.chainIDMul)
	}
	return R, S, V, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (s EIP155Signer) Hash(tx *Transaction) common.Hash {
	return rlpHash([]interface{}{
		tx.data.AccountNonce,
		tx.data.Price,
		tx.data.GasLimit,
		tx.data.Recipient,
		tx.data.Amount,
		tx.data.Payload,
		s.chainID, uint(0), uint(0),
	})
}

func (s EIP155Signer) ChainID() *uint256.Int {
	return s.chainID
}

// HomesteadTransaction implements TransactionInterface using the
// homestead rules.
type HomesteadSigner struct{ FrontierSigner }

func (hs HomesteadSigner) Equal(s2 Signer) bool {
	_, ok := s2.(HomesteadSigner)
	return ok
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (hs HomesteadSigner) SignatureValues(tx *Transaction, sig []byte) (r, s, v *uint256.Int, err error) {
	return hs.FrontierSigner.SignatureValues(tx, sig)
}

func (hs HomesteadSigner) Sender(tx *Transaction) (common.Address, error) {
	return hs.SenderWithContext(secp256k1.DefaultContext, tx)
}

func (hs HomesteadSigner) SenderWithContext(context *secp256k1.Context, tx *Transaction) (common.Address, error) {
	return recoverPlain(context, hs.Hash(tx), &tx.data.R, &tx.data.S, &tx.data.V, true)
}

type FrontierSigner struct{}

func (fs FrontierSigner) Equal(s2 Signer) bool {
	_, ok := s2.(FrontierSigner)
	return ok
}

// SignatureValues returns signature values. This signature
// needs to be in the [R || S || V] format where V is 0 or 1.
func (fs FrontierSigner) SignatureValues(tx *Transaction, sig []byte) (r, s, v *uint256.Int, err error) {
	if len(sig) != crypto.SignatureLength {
		panic(fmt.Sprintf("wrong size for signature: got %d, want %d", len(sig), crypto.SignatureLength))
	}
	r = new(uint256.Int).SetBytes(sig[:32])
	s = new(uint256.Int).SetBytes(sig[32:64])
	v = new(uint256.Int).SetBytes([]byte{sig[64] + 27})
	return r, s, v, nil
}

// Hash returns the hash to be signed by the sender.
// It does not uniquely identify the transaction.
func (fs FrontierSigner) Hash(tx *Transaction) common.Hash {
	return rlpHash([]interface{}{
		tx.data.AccountNonce,
		tx.data.Price,
		tx.data.GasLimit,
		tx.data.Recipient,
		tx.data.Amount,
		tx.data.Payload,
	})
}

func (fs FrontierSigner) Sender(tx *Transaction) (common.Address, error) {
	return fs.SenderWithContext(secp256k1.DefaultContext, tx)
}

func (fs FrontierSigner) SenderWithContext(context *secp256k1.Context, tx *Transaction) (common.Address, error) {
	return recoverPlain(context, fs.Hash(tx), &tx.data.R, &tx.data.S, &tx.data.V, false)
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
func deriveChainID(v *uint256.Int) *uint256.Int {
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
