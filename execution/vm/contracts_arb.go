package vm

import (
	"math/big"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	PrecompiledContractsBeforeArbOS30       = make(PrecompiledContracts)
	PrecompiledAddressesBeforeArbOS30       []accounts.Address
	PrecompiledContractsStartingFromArbOS30 = make(PrecompiledContracts)
	PrecompiledAddressesStartingFromArbOS30 []accounts.Address
	PrecompiledContractsStartingFromArbOS50 = make(PrecompiledContracts)
	PrecompiledAddressesStartingFromArbOS50 []accounts.Address
)

var PrecompiledContractsP256Verify = PrecompiledContracts{
	accounts.InternAddress(common.BytesToAddress([]byte{0x01, 0x00})): &p256Verify{},
}

type AdvancedPrecompileCall struct {
	PrecompileAddress common.Address
	ActingAsAddress   common.Address
	Caller            common.Address
	Value             *big.Int
	ReadOnly          bool
	Evm               *EVM
}

type AdvancedPrecompile interface {
	RunAdvanced(input []byte, suppliedGas uint64, advancedInfo *AdvancedPrecompileCall) (ret []byte, remainingGas uint64, usedMultiGas multigas.MultiGas, err error)
	PrecompiledContract
}

type ArbosAwarePrecompile interface {
	SetArbosVersion(arbosVersion uint64)
	PrecompiledContract
}
