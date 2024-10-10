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
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/secp256k1"

	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/crypto"
)

var ErrInvalidChainId = errors.New("invalid chain id for signer")

// MakeSigner returns a Signer based on the given chain config and block number.
func MakeSigner(config *chain.Config, blockNumber uint64) *Signer {
	var signer Signer
	var chainId uint256.Int
	if config.ChainID != nil {
		overflow := chainId.SetFromBig(config.ChainID)
		if overflow {
			panic(fmt.Errorf("chainID higher than 2^256-1"))
		}
	}
	signer.unprotected = true
	switch {
	case config.IsLondon(blockNumber):
		// All transaction types are still supported
		signer.protected = true
		signer.accesslist = true
		signer.dynamicfee = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Mul(&chainId, u256.Num2)
	case config.IsBerlin(blockNumber):
		signer.protected = true
		signer.accesslist = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Mul(&chainId, u256.Num2)
	case config.IsSpuriousDragon(blockNumber):
		signer.protected = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Mul(&chainId, u256.Num2)
	case config.IsHomestead(blockNumber):
	default:
		// Only allow malleable transactions in Frontier
		signer.maleable = true
	}
	return &signer
}

func MakeFrontierSigner() *Signer {
	var signer Signer
	signer.maleable = true
	signer.unprotected = true
	return &signer
}

// LatestSigner returns the 'most permissive' Signer available for the given chain
// configuration. Specifically, this enables support of EIP-155 replay protection and
// EIP-2930 access list transactions when their respective forks are scheduled to occur at
// any block number in the chain config.
//
// Use this in transaction-handling code where the current block number is unknown. If you
// have the current block number available, use MakeSigner instead.
func LatestSigner(config *chain.Config) *Signer {
	var signer Signer
	signer.unprotected = true
	chainId, overflow := uint256.FromBig(config.ChainID)
	if overflow {
		panic(fmt.Errorf("chainID higher than 2^256-1"))
	}
	signer.chainID.Set(chainId)
	signer.chainIDMul.Mul(chainId, u256.Num2)
	if config.ChainID != nil {
		if config.LondonBlock != nil {
			signer.dynamicfee = true
		}
		if config.BerlinBlock != nil {
			signer.accesslist = true
		}
		if config.SpuriousDragonBlock != nil {
			signer.protected = true
		}
	}
	return &signer
}

// LatestSignerForChainID returns the 'most permissive' Signer available. Specifically,
// this marks support for EIP-155 replay protection, EIP-2718 typed transactions and EIP-1559 dynamic fee
// transaction types if chainID is non-nil.
//
// Use this in transaction-handling code where the current block number and fork
// configuration are unknown. If you have a ChainConfig, use LatestSigner instead.
// If you have a ChainConfig and know the current block number, use MakeSigner instead.
func LatestSignerForChainID(chainID *big.Int) *Signer {
	var signer Signer
	signer.unprotected = true
	if chainID == nil {
		return &signer
	}
	chainId, overflow := uint256.FromBig(chainID)
	if overflow {
		panic(fmt.Errorf("chainID higher than 2^256-1"))
	}
	signer.chainID.Set(chainId)
	signer.chainIDMul.Mul(chainId, u256.Num2)
	signer.protected = true
	signer.accesslist = true
	signer.dynamicfee = true
	return &signer
}

// SignTx signs the transaction using the given signer and private key.
func SignTx(tx Transaction, s Signer, prv *ecdsa.PrivateKey) (Transaction, error) {
	h := tx.SigningHash(s.chainID.ToBig())
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

// SignNewTx creates a transaction and signs it.
func SignNewTx(prv *ecdsa.PrivateKey, s Signer, tx Transaction) (Transaction, error) {
	h := tx.SigningHash(s.chainID.ToBig())
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(s, sig)
}

// MustSignNewTx creates a transaction and signs it.
// This panics if the transaction cannot be signed.
func MustSignNewTx(prv *ecdsa.PrivateKey, s Signer, tx Transaction) Transaction {
	tx1, err := SignNewTx(prv, s, tx)
	if err != nil {
		panic(err)
	}
	return tx1
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
	return fmt.Sprintf("Signer[chainId=%s,malleable=%t,unprotected=%t,protected=%t,accesslist=%t,dynamicfee=%t", &sg.chainID, sg.maleable, sg.unprotected, sg.protected, sg.accesslist, sg.dynamicfee)
}

// Sender returns the sender address of the transaction.
func (sg Signer) Sender(tx Transaction) (libcommon.Address, error) {
	return sg.SenderWithContext(secp256k1.DefaultContext, tx)
}

// SenderWithContext returns the sender address of the transaction.
func (sg Signer) SenderWithContext(context *secp256k1.Context, tx Transaction) (libcommon.Address, error) {
	var V uint256.Int
	var R, S *uint256.Int
	signChainID := sg.chainID.ToBig() // This is reset to nil if tx is unprotected
	// recoverPlain below will subract 27 from V
	switch t := tx.(type) {
	case *LegacyTx:
		if !t.Protected() {
			if !sg.unprotected {
				return libcommon.Address{}, fmt.Errorf("unprotected tx is not supported by signer %s", sg)
			}
			signChainID = nil
			V.Set(&t.V)
		} else {
			if !sg.protected {
				return libcommon.Address{}, fmt.Errorf("protected tx is not supported by signer %s", sg)
			}
			if !DeriveChainId(&t.V).Eq(&sg.chainID) {
				return libcommon.Address{}, ErrInvalidChainId
			}
			V.Sub(&t.V, &sg.chainIDMul)
			V.Sub(&V, u256.Num8)
		}
		R, S = &t.R, &t.S
	case *AccessListTx:
		if !sg.accesslist {
			return libcommon.Address{}, fmt.Errorf("accesslist tx is not supported by signer %s", sg)
		}
		if t.ChainID == nil {
			if !sg.chainID.IsZero() {
				return libcommon.Address{}, ErrInvalidChainId
			}
		} else if !t.ChainID.Eq(&sg.chainID) {
			return libcommon.Address{}, ErrInvalidChainId
		}
		// ACL txs are defined to use 0 and 1 as their recovery id, add
		// 27 to become equivalent to unprotected Homestead signatures.
		V.Add(&t.V, u256.Num27)
		R, S = &t.R, &t.S
	case *DynamicFeeTransaction:
		if !sg.dynamicfee {
			return libcommon.Address{}, fmt.Errorf("dynamicfee tx is not supported by signer %s", sg)
		}
		if t.ChainID == nil {
			if !sg.chainID.IsZero() {
				return libcommon.Address{}, ErrInvalidChainId
			}
		} else if !t.ChainID.Eq(&sg.chainID) {
			return libcommon.Address{}, ErrInvalidChainId
		}
		// ACL and DynamicFee txs are defined to use 0 and 1 as their recovery
		// id, add 27 to become equivalent to unprotected Homestead signatures.
		V.Add(&t.V, u256.Num27)
		R, S = &t.R, &t.S
	default:
		return libcommon.Address{}, ErrTxTypeNotSupported
	}
	return recoverPlain(context, tx.SigningHash(signChainID), R, S, &V, !sg.maleable)
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
		if t.ChainID != nil && !t.ChainID.IsZero() && !t.ChainID.Eq(&sg.chainID) {
			return nil, nil, nil, ErrInvalidChainId
		}
		R, S, V = decodeSignature(sig)
	case *DynamicFeeTransaction:
		// Check that chain ID of tx matches the signer. We also accept ID zero here,
		// because it indicates that the chain ID was not specified in the tx.
		if t.ChainID != nil && !t.ChainID.IsZero() && !t.ChainID.Eq(&sg.chainID) {
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

// Equal returns true if the given signer is the same as the receiver.
func (sg Signer) Equal(other Signer) bool {
	return sg.chainID.Eq(&other.chainID) &&
		sg.maleable == other.maleable &&
		sg.unprotected == other.unprotected &&
		sg.protected == other.protected &&
		sg.accesslist == other.accesslist &&
		sg.dynamicfee == other.dynamicfee
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

func recoverPlain(context *secp256k1.Context, sighash libcommon.Hash, R, S, Vb *uint256.Int, homestead bool) (libcommon.Address, error) {
	if Vb.BitLen() > 8 {
		return libcommon.Address{}, ErrInvalidSig
	}
	V := byte(Vb.Uint64() - 27)
	if !crypto.ValidateSignatureValues(V, R, S, homestead) {
		return libcommon.Address{}, ErrInvalidSig
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
		return libcommon.Address{}, err
	}
	if len(pub) == 0 || pub[0] != 4 {
		return libcommon.Address{}, errors.New("invalid public key")
	}
	var addr libcommon.Address
	copy(addr[:], crypto.Keccak256(pub[1:])[12:])
	return addr, nil
}

// deriveChainID derives the chain id from the given v parameter
func DeriveChainId(v *uint256.Int) *uint256.Int {
	if v.IsUint64() {
		v := v.Uint64()
		if v == 27 || v == 28 {
			return new(uint256.Int)
		}
		return new(uint256.Int).SetUint64((v - 35) / 2)
	}
	r := new(uint256.Int).Sub(v, u256.Num35)
	return r.Div(r, u256.Num2)
}
