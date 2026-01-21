package types

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

var ArbosAddress = accounts.InternAddress(common.HexToAddress("0xa4b05"))
var ArbosStateAddress = accounts.InternAddress(common.HexToAddress("0xA4B05FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"))
var ArbSysAddress = accounts.InternAddress(common.HexToAddress("0x64"))
var ArbInfoAddress = accounts.InternAddress(common.HexToAddress("0x65"))
var ArbAddressTableAddress = accounts.InternAddress(common.HexToAddress("0x66"))
var ArbBLSAddress = accounts.InternAddress(common.HexToAddress("0x67"))
var ArbFunctionTableAddress = accounts.InternAddress(common.HexToAddress("0x68"))
var ArbosTestAddress = accounts.InternAddress(common.HexToAddress("0x69"))
var ArbGasInfoAddress = accounts.InternAddress(common.HexToAddress("0x6c"))
var ArbOwnerPublicAddress = accounts.InternAddress(common.HexToAddress("0x6b"))
var ArbAggregatorAddress = accounts.InternAddress(common.HexToAddress("0x6d"))
var ArbRetryableTxAddress = common.HexToAddress("0x6e")
var ArbStatisticsAddress = accounts.InternAddress(common.HexToAddress("0x6f"))
var ArbOwnerAddress = accounts.InternAddress(common.HexToAddress("0x70"))
var ArbWasmAddress = accounts.InternAddress(common.HexToAddress("0x71"))
var ArbWasmCacheAddress = accounts.InternAddress(common.HexToAddress("0x72"))
var ArbNativeTokenManagerAddress = accounts.InternAddress(common.HexToAddress("0x73"))
var NodeInterfaceAddress = accounts.InternAddress(common.HexToAddress("0xc8"))
var NodeInterfaceDebugAddress = accounts.InternAddress(common.HexToAddress("0xc9"))
var ArbDebugAddress = accounts.InternAddress(common.HexToAddress("0xff"))

type ArbitrumSigner struct {
	Signer
}

func NewArbitrumSigner(signer Signer) ArbitrumSigner {
	return ArbitrumSigner{Signer: signer}
}

func (s ArbitrumSigner) Sender(tx Transaction) (accounts.Address, error) {
	switch inner := tx.(type) {
	case *ArbitrumUnsignedTx:
		return inner.From, nil
	case *ArbitrumContractTx:
		return inner.From, nil
	case *ArbitrumDepositTx:
		return inner.From, nil
	case *ArbitrumInternalTx:
		return ArbosAddress, nil
	case *ArbitrumRetryTx:
		return inner.From, nil
	case *ArbitrumSubmitRetryableTx:
		return inner.From, nil
	case *ArbitrumLegacyTxData:
		if inner.OverrideSender != nil {
			return *inner.OverrideSender, nil
		}
		// TODO Arbitrum: not sure this check is needed for arb1
		if inner.LegacyTx.V.IsZero() && inner.LegacyTx.R.IsZero() && inner.LegacyTx.S.IsZero() {
			return common.Address{}, nil
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
