package types

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

var ArbosAddress = common.HexToAddress("0xa4b05")
var ArbosStateAddress = common.HexToAddress("0xA4B05FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
var ArbSysAddress = common.HexToAddress("0x64")
var ArbInfoAddress = common.HexToAddress("0x65")
var ArbAddressTableAddress = common.HexToAddress("0x66")
var ArbBLSAddress = common.HexToAddress("0x67")
var ArbFunctionTableAddress = common.HexToAddress("0x68")
var ArbosTestAddress = common.HexToAddress("0x69")
var ArbGasInfoAddress = common.HexToAddress("0x6c")
var ArbOwnerPublicAddress = common.HexToAddress("0x6b")
var ArbAggregatorAddress = common.HexToAddress("0x6d")
var ArbRetryableTxAddress = common.HexToAddress("0x6e")
var ArbStatisticsAddress = common.HexToAddress("0x6f")
var ArbOwnerAddress = common.HexToAddress("0x70")
var ArbWasmAddress = common.HexToAddress("0x71")
var ArbWasmCacheAddress = common.HexToAddress("0x72")
var ArbNativeTokenManagerAddress = common.HexToAddress("0x73")
var NodeInterfaceAddress = common.HexToAddress("0xc8")
var NodeInterfaceDebugAddress = common.HexToAddress("0xc9")
var ArbDebugAddress = common.HexToAddress("0xff")

type ArbitrumSigner struct {
	Signer
}

func NewArbitrumSigner(signer Signer) ArbitrumSigner {
	return ArbitrumSigner{Signer: signer}
}

func (s ArbitrumSigner) Sender(tx Transaction) (accounts.Address, error) {
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
		// TODO Arbitrum: not sure this check is needed for arb1
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

func (s ArbitrumSigner) SignatureValues(tx Transaction, sig []byte) (R, S, V *uint256.Int, err error) {
	switch dataTx := tx.(type) {
	case *ArbitrumUnsignedTx, *ArbitrumContractTx, *ArbitrumDepositTx,
		*ArbitrumInternalTx, *ArbitrumRetryTx, *ArbitrumSubmitRetryableTx:

		return nil, nil, nil, nil
	case *ArbitrumLegacyTxData:
		// legacyData := tx.(*ArbitrumLegacyTxData)
		// fakeTx := NewArbTx(legacyData.LegacyTx)
		return s.Signer.SignatureValues(dataTx.LegacyTx, sig)
	default:
		return s.Signer.SignatureValues(tx, sig)
	}
}
