package vm

import (
	"math/big"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
)

var (
	PrecompiledContractsBeforeArbOS30       = make(map[common.Address]PrecompiledContract)
	PrecompiledAddressesBeforeArbOS30       []common.Address
	PrecompiledContractsStartingFromArbOS30 = make(map[common.Address]PrecompiledContract)
	PrecompiledAddressesStartingFromArbOS30 []common.Address
	PrecompiledContractsStartingFromArbOS50 = make(map[common.Address]PrecompiledContract)
	PrecompiledAddressesStartingFromArbOS50 []common.Address
)

var PrecompiledContractsP256Verify = map[common.Address]PrecompiledContract{
	common.BytesToAddress([]byte{0x01, 0x00}): &p256Verify{},
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

// TODO move into arbitrum package
type ArbosAwarePrecompile interface {
	SetArbosVersion(arbosVersion uint64)
	PrecompiledContract
}
