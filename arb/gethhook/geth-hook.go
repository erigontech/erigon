// Copyright 2021-2022, Offchain Labs, Inc.
// For license information, see https://github.com/nitro/blob/master/LICENSE

package gethhook

import (
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/nitro-erigon/arbos"
	"github.com/erigontech/nitro-erigon/execution/erigon/core_hooks"
	"github.com/erigontech/nitro-erigon/precompiles"
)

type ArbosPrecompileWrapper struct {
	inner precompiles.ArbosPrecompile
}

func (p ArbosPrecompileWrapper) RequiredGas(input []byte) uint64 {
	panic("Non-advanced precompile method called")
}

func (p ArbosPrecompileWrapper) Run(input []byte) ([]byte, error) {
	panic("Non-advanced precompile method called")
}

func (p ArbosPrecompileWrapper) Name() string {
	return p.inner.Name()
}

func (p ArbosPrecompileWrapper) RunAdvanced(
	input []byte,
	gasSupplied uint64,
	info *vm.AdvancedPrecompileCall,
) (ret []byte, gasLeft uint64, usedMultiGas multigas.MultiGas, err error) {

	// Precompiles don't actually enter evm execution like normal calls do,
	// so we need to increment the depth here to simulate the callstack change.
	info.Evm.Interpreter().IncDepth()
	defer info.Evm.Interpreter().DecDepth()

	return p.inner.Call(
		input, info.PrecompileAddress, info.ActingAsAddress,
		info.Caller, info.Value, info.ReadOnly, gasSupplied, info.Evm,
	)
}

func init() {
	core_hooks.ReadyEVMForL2 = func(evm *vm.EVM, msg *types.Message) {
		if evm.ChainConfig().IsArbitrum() {
			evm.ProcessingHook = arbos.NewTxProcessor(evm, msg)
		}
	}

	// process arbos precompiles
	precompileErrors := make(map[[4]byte]abi.Error)
	for addr, precompile := range precompiles.Precompiles() {
		for _, errABI := range precompile.Precompile().GetErrorABIs() {
			precompileErrors[[4]byte(errABI.ID.Bytes())] = errABI
		}

		// Arbos is a special case, Arbos handles versioning of precompiles internally, versioning of Ethereum/non-Arbos precompiles must be handled externally
		var wrapped vm.AdvancedPrecompile = ArbosPrecompileWrapper{precompile}
		vm.PrecompiledContractsBeforeArbOS30[addr] = wrapped
		vm.PrecompiledContractsStartingFromArbOS30[addr] = wrapped
		vm.PrecompiledContractsStartingFromArbOS50[addr] = wrapped
	}

	// process Ethereum precompiles for respective arbos versions
	addPrecompiles(vm.PrecompiledContractsBeforeArbOS30, vm.PrecompiledContractsBerlin)
	addPrecompiles(vm.PrecompiledContractsStartingFromArbOS30, vm.PrecompiledContractsCancun)
	addPrecompiles(vm.PrecompiledContractsStartingFromArbOS30, vm.PrecompiledContractsP256Verify)
	addPrecompiles(vm.PrecompiledContractsStartingFromArbOS50, vm.PrecompiledContractsOsaka)

	// process addresses for respective arbos version precompile maps
	addAddresses(&vm.PrecompiledAddressesBeforeArbOS30, vm.PrecompiledContractsBeforeArbOS30)
	addAddresses(&vm.PrecompiledAddressesStartingFromArbOS30, vm.PrecompiledContractsStartingFromArbOS30)
	addAddresses(&vm.PrecompiledAddressesStartingFromArbOS50, vm.PrecompiledContractsStartingFromArbOS50)

	core_hooks.RenderRPCError = func(data []byte) error {
		if len(data) < 4 {
			return nil
		}
		var id [4]byte
		copy(id[:], data[:4])
		errABI, found := precompileErrors[id]
		if !found {
			return nil
		}
		rendered, err := precompiles.RenderSolError(errABI, data)
		if err != nil {
			log.Warn("failed to render rpc error", "err", err)
			return nil
		}
		return errors.New(rendered)
	}
}

func addPrecompiles(contracts map[common.Address]vm.PrecompiledContract, toAdd map[common.Address]vm.PrecompiledContract) {
	for addr, precompile := range toAdd {
		contracts[addr] = precompile
	}
}

func addAddresses(addresses *[]common.Address, toAdd map[common.Address]vm.PrecompiledContract) {
	for addr := range toAdd {
		*addresses = append(*addresses, addr)
	}
}

// RequireHookedGeth does nothing, but forces an import to let the init function run
func RequireHookedGeth() {}
