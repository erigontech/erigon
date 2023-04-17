package state

import (
	"errors"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
)

var (
	// ErrInvalidSig indicates the signature of the transaction is not valid
	ErrInvalidSig = errors.New("invalid transaction v, r, s values")
)

// CheckSignature checks a transaction signature
func CheckSignature(tx types.Transaction) error {
	// Check Signature
	v, r, s := tx.RawSignatureValues()
	plainV := byte(0)
	chainID := tx.GetChainID().Uint64()
	if chainID != 0 {
		plainV = byte(v.Uint64() - 35 - 2*(chainID))
	}
	if !crypto.ValidateSignatureValues(plainV, r, s, false) {
		return ErrInvalidSig
	}

	return nil
}
