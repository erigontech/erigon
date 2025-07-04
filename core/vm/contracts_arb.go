package vm

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
)

var (
	PrecompiledContractsArbitrum = make(map[common.Address]PrecompiledContract)
	PrecompiledAddressesArbitrum []common.Address
	PrecompiledContractsArbOS30  = make(map[common.Address]PrecompiledContract)
	PrecompiledAddressesArbOS30  []common.Address
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
	RunAdvanced(input []byte, suppliedGas uint64, advancedInfo *AdvancedPrecompileCall) (ret []byte, remainingGas uint64, err error)
	PrecompiledContract
}
