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
	"github.com/ledgerwatch/secp256k1"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/crypto"
)

var ErrInvalidChainId = errors.New("invalid chain id for signer")

// MakeSigner returns a Signer based on the given chain config and block number.
func MakeSigner(config *chain.Config, blockNumber uint64, blockTime uint64) *Signer {
	var signer Signer
	chainId, overflow := uint256.FromBig(config.ChainID)
	if overflow {
		panic(fmt.Errorf("chainID higher than 2^256-1"))
	}
	signer.unprotected = true
	switch {
	case config.IsCancun(blockTime):
		// All transaction types are still supported
		signer.protected = true
		signer.accessList = true
		signer.dynamicFee = true
		signer.blob = true
		signer.chainID = chainId
	case config.IsLondon(blockNumber):
		signer.protected = true
		signer.accessList = true
		signer.dynamicFee = true
		signer.chainID = chainId
	case config.IsBerlin(blockNumber):
		signer.protected = true
		signer.accessList = true
		signer.chainID = chainId
	case config.IsSpuriousDragon(blockNumber):
		signer.protected = true
		signer.chainID = chainId
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
		panic(fmt.Errorf("chainID higher than 2^256-1"))
	}
	signer.chainID = chainId
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
	signer.chainID = chainId
	signer.protected = true
	signer.accessList = true
	signer.dynamicFee = true
	signer.blob = true
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
	chainID     *uint256.Int
	malleable   bool // Whether this signer should allow legacy transactions with malleability
	unprotected bool // Whether this signer should allow legacy transactions without chainId protection
	protected   bool // Whether this signer should allow transactions with replay protection via chainId
	accessList  bool // Whether this signer should allow transactions with access list, supersedes protected
	dynamicFee  bool // Whether this signer should allow transactions with base fee and tip (instead of gasprice), supersedes accessList
	blob        bool // Whether this signer should allow blob transactions
}

func (sg Signer) String() string {
	return fmt.Sprintf("Signer[chainId=%s,malleable=%t,unprotected=%t,protected=%t,accessList=%t,dynamicFee=%t,blob=%t",
		sg.chainID, sg.malleable, sg.unprotected, sg.protected, sg.accessList, sg.dynamicFee, sg.blob)
}

// Sender returns the sender address of the transaction.
func (sg Signer) Sender(tx Transaction) (libcommon.Address, error) {
	return sg.SenderWithContext(secp256k1.DefaultContext, tx)
}

// SenderWithContext returns the sender address of the transaction.
func (sg Signer) SenderWithContext(context *secp256k1.Context, tx Transaction) (libcommon.Address, error) {
	signChainID := sg.chainID.ToBig() // This is reset to nil if tx is unprotected
	if !tx.ReplayProtected() {
		if !sg.unprotected {
			return libcommon.Address{}, fmt.Errorf("unprotected tx is not supported by signer %s", sg)
		}
		signChainID = nil
	} else {
		if !sg.protected {
			return libcommon.Address{}, fmt.Errorf("protected tx is not supported by signer %s", sg)
		}
	}

	switch tx.(type) {
	case *LegacyTx:
		// do nothing
	case *AccessListTx:
		if !sg.accessList {
			return libcommon.Address{}, fmt.Errorf("accessList tx is not supported by signer %s", sg)
		}
	case *DynamicFeeTransaction:
		if !sg.dynamicFee {
			return libcommon.Address{}, fmt.Errorf("dynamicFee tx is not supported by signer %s", sg)
		}
	case *BlobTx:
		if !sg.blob {
			return libcommon.Address{}, fmt.Errorf("blob tx is not supported by signer %s", sg)
		}
	default:
		return libcommon.Address{}, ErrTxTypeNotSupported
	}
	yParity, r, s := tx.RawSignatureValues()
	return recoverPlain(context, tx.SigningHash(signChainID), yParity, r, s, !sg.malleable)
}

func (sg Signer) SignatureValues(tx Transaction, sig []byte) (r, s *uint256.Int, yParity bool, err error) {
	// Check that chain ID of tx matches the signer.
	if tx.GetChainID() != nil && sg.chainID != nil && !tx.GetChainID().Eq(sg.chainID) {
		return nil, nil, false, ErrInvalidChainId
	}

	var y byte
	r, s, y = decodeSignature(sig)
	if y != 0 && y != 1 {
		return nil, nil, false, ErrInvalidSig
	}
	yParity = y != 0
	return
}

func (sg Signer) ChainID() *uint256.Int {
	return sg.chainID
}

/*
// Equal returns true if the given signer is the same as the receiver.
func (sg Signer) Equal(other Signer) bool {
	return sg.chainID.Eq(&other.chainID) &&
		sg.malleable == other.malleable &&
		sg.unprotected == other.unprotected &&
		sg.protected == other.protected &&
		sg.accessList == other.accessList &&
		sg.dynamicFee == other.dynamicFee &&
		sg.blob == other.blob
}
*/

func decodeSignature(sig []byte) (r, s *uint256.Int, yParity byte) {
	if len(sig) != crypto.SignatureLength {
		panic(fmt.Sprintf("wrong size for signature: got %d, want %d", len(sig), crypto.SignatureLength))
	}
	r = new(uint256.Int).SetBytes(sig[:32])
	s = new(uint256.Int).SetBytes(sig[32:64])
	yParity = sig[64]
	return r, s, yParity
}

func recoverPlain(context *secp256k1.Context, sighash libcommon.Hash, yParity bool, R, S *uint256.Int, homestead bool,
) (libcommon.Address, error) {
	var v byte
	if yParity {
		v = 1
	}
	if !crypto.ValidateSignatureValues(v, R, S, homestead) {
		return libcommon.Address{}, ErrInvalidSig
	}
	// encode the signature in uncompressed format
	r, s := R.Bytes(), S.Bytes()
	sig := make([]byte, crypto.SignatureLength)
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)
	sig[64] = v
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

// See EIP-155: Simple replay attack protection
func DeriveChainIdAndYParity(v *uint256.Int) (chainId *uint256.Int, yParity bool, err error) {
	if v.CmpUint64(27) == 0 {
		return nil, false, nil
	} else if v.CmpUint64(28) == 0 {
		return nil, true, nil
	} else if v.CmpUint64(35) < 0 {
		return nil, false, ErrInvalidSig
	}

	// Find chainId and yParity ∈ {0, 1} such that
	// v = chainId * 2 + 35 + yParity
	r := new(uint256.Int).SubUint64(v, 35) // r := v - 35
	yParity = (r.Uint64() % 2) != 0        // = r mod 2
	chainId = new(uint256.Int).Rsh(r, 1)   // = r/2
	return chainId, yParity, nil
}
