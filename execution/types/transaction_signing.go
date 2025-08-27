// Copyright 2016 The go-ethereum Authors
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

package types

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/secp256k1"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain"
)

var ErrInvalidChainId = errors.New("invalid chain id for signer")

// MakeSigner returns a Signer based on the given chain config and block number.
func MakeSigner(config *chain.Config, blockNumber uint64, blockTime uint64) *Signer {
	var signer Signer
	var chainId uint256.Int
	if config.ChainID != nil {
		overflow := chainId.SetFromBig(config.ChainID)
		if overflow {
			panic("chainID higher than 2^256-1")
		}
	}
	signer.unprotected = true
	switch {
	case config.IsPrague(blockTime):
		signer.protected = true
		signer.accessList = true
		signer.dynamicFee = true
		signer.blob = true
		signer.setCode = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Lsh(&chainId, 1) // ×2
	case config.IsBhilai(blockNumber):
		signer.protected = true
		signer.accessList = true
		signer.dynamicFee = true
		signer.blob = false
		signer.setCode = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Lsh(&chainId, 1) // ×2
	case config.IsCancun(blockTime):
		// All transaction types are still supported
		signer.protected = true
		signer.accessList = true
		signer.dynamicFee = true
		signer.blob = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Lsh(&chainId, 1) // ×2
	case config.IsLondon(blockNumber):
		signer.protected = true
		signer.accessList = true
		signer.dynamicFee = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Lsh(&chainId, 1) // ×2
	case config.IsBerlin(blockNumber):
		signer.protected = true
		signer.accessList = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Lsh(&chainId, 1) // ×2
	case config.IsSpuriousDragon(blockNumber):
		signer.protected = true
		signer.chainID.Set(&chainId)
		signer.chainIDMul.Lsh(&chainId, 1) // ×2
	case config.IsHomestead(blockNumber):
	default:
		// Only allow malleable transactions in Frontier
		signer.malleable = true
	}
	return &signer
}

func MakeFrontierSigner() *Signer {
	var signer Signer
	signer.malleable = true
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
		panic("chainID higher than 2^256-1")
	}
	signer.chainID.Set(chainId)
	signer.chainIDMul.Lsh(chainId, 1) // ×2
	if config.ChainID != nil {
		if config.CancunTime != nil {
			signer.blob = true
		}
		if config.LondonBlock != nil {
			signer.dynamicFee = true
		}
		if config.BerlinBlock != nil {
			signer.accessList = true
		}
		if config.SpuriousDragonBlock != nil {
			signer.protected = true
		}
		if config.PragueTime != nil {
			signer.setCode = true
		}
		if config.Bor != nil && config.Bor.GetBhilaiBlock() != nil {
			signer.setCode = true
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
		panic("chainID higher than 2^256-1")
	}
	signer.chainID.Set(chainId)
	signer.chainIDMul.Lsh(chainId, 1) // ×2
	signer.protected = true
	signer.accessList = true
	signer.dynamicFee = true
	signer.blob = true
	signer.setCode = true
	return &signer
}

// SignTx signs the transaction using the given signer and private key.
func SignTx(txn Transaction, s Signer, prv *ecdsa.PrivateKey) (Transaction, error) {
	h := txn.SigningHash(s.chainID.ToBig())
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return txn.WithSignature(s, sig)
}

// SignNewTx creates a transaction and signs it.
func SignNewTx(prv *ecdsa.PrivateKey, s Signer, txn Transaction) (Transaction, error) {
	h := txn.SigningHash(s.chainID.ToBig())
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return txn.WithSignature(s, sig)
}

// MustSignNewTx creates a transaction and signs it.
// This panics if the transaction cannot be signed.
func MustSignNewTx(prv *ecdsa.PrivateKey, s Signer, txn Transaction) Transaction {
	tx1, err := SignNewTx(prv, s, txn)
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
	malleable           bool // Whether this signer should allow legacy transactions with malleability
	unprotected         bool // Whether this signer should allow legacy transactions without chainId protection
	protected           bool // Whether this signer should allow transactions with replay protection via chainId
	accessList          bool // Whether this signer should allow transactions with access list, supersedes protected
	dynamicFee          bool // Whether this signer should allow transactions with base fee and tip (instead of gasprice), supersedes accessList
	blob                bool // Whether this signer should allow blob transactions
	setCode             bool // Whether this signer should allow set code transactions
}

func (sg Signer) String() string {
	return fmt.Sprintf("Signer[chainId=%s,malleable=%t,unprotected=%t,protected=%t,accessList=%t,dynamicFee=%t,blob=%t,setCode=%t",
		&sg.chainID, sg.malleable, sg.unprotected, sg.protected, sg.accessList, sg.dynamicFee, sg.blob, sg.setCode)
}

// Sender returns the sender address of the transaction.
func (sg Signer) Sender(tx Transaction) (common.Address, error) {
	return sg.SenderWithContext(secp256k1.DefaultContext, tx)
}

// SenderWithContext returns the sender address of the transaction.
func (sg Signer) SenderWithContext(context *secp256k1.Context, txn Transaction) (common.Address, error) {
	var V uint256.Int
	var R, S *uint256.Int
	signChainID := sg.chainID.ToBig() // This is reset to nil if txn is unprotected
	// recoverPlain below will subtract 27 from V
	switch t := txn.(type) {
	case *LegacyTx:
		if !t.Protected() {
			if !sg.unprotected {
				return common.Address{}, fmt.Errorf("unprotected txn is not supported by signer %s", sg)
			}
			signChainID = nil
			V.Set(&t.V)
		} else {
			if !sg.protected {
				return common.Address{}, fmt.Errorf("protected txn is not supported by signer %s", sg)
			}
			if !DeriveChainId(&t.V).Eq(&sg.chainID) {
				return common.Address{}, ErrInvalidChainId
			}
			V.Sub(&t.V, &sg.chainIDMul)
			V.Sub(&V, u256.Num8)
		}
		R, S = &t.R, &t.S
	case *AccessListTx:
		if !sg.accessList {
			return common.Address{}, fmt.Errorf("accessList txn is not supported by signer %s", sg)
		}
		if t.ChainID == nil {
			if !sg.chainID.IsZero() {
				return common.Address{}, ErrInvalidChainId
			}
		} else if !t.ChainID.Eq(&sg.chainID) {
			return common.Address{}, ErrInvalidChainId
		}
		// ACL txs are defined to use 0 and 1 as their recovery id, add
		// 27 to become equivalent to unprotected Homestead signatures.
		V.Add(&t.V, u256.Num27)
		R, S = &t.R, &t.S
	case *DynamicFeeTransaction:
		if !sg.dynamicFee {
			return common.Address{}, fmt.Errorf("dynamicFee txn is not supported by signer %s", sg)
		}
		if t.ChainID == nil {
			if !sg.chainID.IsZero() {
				return common.Address{}, ErrInvalidChainId
			}
		} else if !t.ChainID.Eq(&sg.chainID) {
			return common.Address{}, ErrInvalidChainId
		}
		// ACL and DynamicFee txs are defined to use 0 and 1 as their recovery
		// id, add 27 to become equivalent to unprotected Homestead signatures.
		V.Add(&t.V, u256.Num27)
		R, S = &t.R, &t.S
	case *BlobTx:
		if !sg.blob {
			return common.Address{}, fmt.Errorf("blob txn is not supported by signer %s", sg)
		}
		if t.ChainID == nil {
			if !sg.chainID.IsZero() {
				return common.Address{}, ErrInvalidChainId
			}
		} else if !t.ChainID.Eq(&sg.chainID) {
			return common.Address{}, ErrInvalidChainId
		}
		// ACL, DynamicFee, and blob txs are defined to use 0 and 1 as their recovery
		// id, add 27 to become equivalent to unprotected Homestead signatures.
		V.Add(&t.V, u256.Num27)
		R, S = &t.R, &t.S
	case *SetCodeTransaction:
		if !sg.setCode {
			return common.Address{}, fmt.Errorf("setCode tx is not supported by signer %s", sg)
		}
		if t.ChainID == nil {
			if !sg.chainID.IsZero() {
				return common.Address{}, ErrInvalidChainId
			}
		} else if !t.ChainID.Eq(&sg.chainID) {
			return common.Address{}, ErrInvalidChainId
		}
		// ACL, DynamicFee, blob, and setCode txs are defined to use 0 and 1 as their recovery
		// id, add 27 to become equivalent to unprotected Homestead signatures.
		V.Add(&t.V, u256.Num27)
		R, S = &t.R, &t.S
	case *AccountAbstractionTransaction:
		return txn.Sender(Signer{})
	default:
		return common.Address{}, ErrTxTypeNotSupported
	}
	return recoverPlain(context, txn.SigningHash(signChainID), R, S, &V, !sg.malleable)
}

// SignatureValues returns the raw R, S, V values corresponding to the
// given signature.
func (sg Signer) SignatureValues(txn Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	switch t := txn.(type) {
	case *LegacyTx:
		R, S, V = decodeSignature(sig)
		if sg.chainID.IsZero() {
			V.Add(V, u256.Num27)
		} else {
			V.Add(V, u256.Num35)
			V.Add(V, &sg.chainIDMul)
		}
	case *DynamicFeeTransaction, *AccessListTx, *BlobTx, *SetCodeTransaction:
		// Check that chain ID of tx matches the signer. We also accept ID zero here,
		// because it indicates that the chain ID was not specified in the tx.
		chainId := t.GetChainID()
		if chainId != nil && !chainId.IsZero() && !chainId.Eq(&sg.chainID) {
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
		sg.malleable == other.malleable &&
		sg.unprotected == other.unprotected &&
		sg.protected == other.protected &&
		sg.accessList == other.accessList &&
		sg.dynamicFee == other.dynamicFee &&
		sg.blob == other.blob &&
		sg.setCode == other.setCode
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

func recoverPlain(context *secp256k1.Context, sighash common.Hash, R, S, Vb *uint256.Int, homestead bool) (common.Address, error) {
	if Vb.BitLen() > 8 {
		return common.Address{}, ErrInvalidSig
	}
	V := byte(Vb.Uint64() - 27)
	if !crypto.TransactionSignatureIsValid(V, R, S, !homestead) {
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
func DeriveChainId(v *uint256.Int) *uint256.Int {
	if v.IsUint64() {
		v := v.Uint64()
		if v == 27 || v == 28 {
			return new(uint256.Int)
		}
		return new(uint256.Int).SetUint64((v - 35) / 2)
	}
	r := new(uint256.Int).Sub(v, u256.Num35)
	return r.Rsh(r, 1) // ÷2
}
