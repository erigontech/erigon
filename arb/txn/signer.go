package txn

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

type ArbitrumSigner struct {
	types.Signer
}

func NewArbitrumSigner(signer types.Signer) ArbitrumSigner {
	return ArbitrumSigner{Signer: signer}
}

func (s ArbitrumSigner) Sender(tx types.Transaction) (accounts.Address, error) {
	switch inner := tx.(type) {
	case *ArbitrumUnsignedTx:
		return accounts.InternAddress(inner.From), nil
	case *ArbitrumContractTx:
		return accounts.InternAddress(inner.From), nil
	case *ArbitrumDepositTx:
		return accounts.InternAddress(inner.From), nil
	case *ArbitrumInternalTx:
		return accounts.InternAddress(ArbosAddress), nil
	case *ArbitrumRetryTx:
		return accounts.InternAddress(inner.From), nil
	case *ArbitrumSubmitRetryableTx:
		return accounts.InternAddress(inner.From), nil
	case *ArbitrumLegacyTxData:
		if inner.OverrideSender != nil {
			return accounts.InternAddress(*inner.OverrideSender), nil
		}
		if inner.LegacyTx.V.IsZero() && inner.LegacyTx.R.IsZero() && inner.LegacyTx.S.IsZero() {
			return accounts.InternAddress(common.Address{}), nil
		}
		return s.Signer.Sender(inner.LegacyTx)
	default:
		return s.Signer.Sender(tx)
	}
}

func (s ArbitrumSigner) Equal(s2 ArbitrumSigner) bool {
	// x, ok := s2.(ArbitrumSigner)
	return s2.Signer.Equal(s.Signer)
}

func (s ArbitrumSigner) SignatureValues(tx types.Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	switch tx.(type) {
	case *ArbitrumUnsignedTx, *ArbitrumContractTx, *ArbitrumDepositTx,
		*ArbitrumInternalTx, *ArbitrumRetryTx, *ArbitrumSubmitRetryableTx:

		return nil, nil, nil, nil
	case *ArbitrumLegacyTxData:
		legacyData := tx.(*ArbitrumLegacyTxData)
		fakeTx := NewArbTx(legacyData.LegacyTx)
		return s.Signer.SignatureValues(fakeTx, sig)
	default:
		return s.Signer.SignatureValues(tx, sig)
	}
}
