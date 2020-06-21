package main

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/params"
	"math/big"
)

func testGenCfg() error {
	env := vm.NewEVM(vm.Context{BlockNumber: big.NewInt(1)}, &dummyStatedb{}, params.TestChainConfig,
		vm.Config{
			EVMInterpreter: "SaInterpreter",
		}, nil)

	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x1, 0x0}
	//contract.Code = []byte{byte(vm.ADD), 0x1, 0x1, 0x0}

	_, err := env.Interpreter().Run(contract, []byte{}, false)
	if err != nil {
		return err
	}

	print("Done")
	return nil
}