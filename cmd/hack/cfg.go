package main

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"math/big"
)

func testGenCfg() error {
	//cfg0Test0()
	//cfg0Test1()
	//dfTest1()
	//dfTest2()
	//dfTest3()
	absIntTest1()
	//absIntTest3()
	return nil
}

func cfg0Test0() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x1, 0x0}
	vm.Cfg0Harness(contract)
}

func cfg0Test1() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x2, byte(vm.PUSH1), 0x0, byte(vm.JUMP), 0x0}
	vm.Cfg0Harness(contract)
}

func dfTest0() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x2, byte(vm.PUSH1), 0x0, 0x0}
	vm.SimpleConstPropHarness(contract)
}

func dfTest1() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x2, byte(vm.PUSH1), 0x0, byte(vm.JUMP), 0x0}
	vm.SimpleConstPropHarness(contract)
}

func dfTest2() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x2, byte(vm.PUSH1), 0x6, byte(vm.JUMP), 0x0}
	vm.SimpleConstPropHarness(contract)
}

func dfTest3() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.JUMP), 0x0}
	vm.SimpleConstPropHarness(contract)
}

func absIntTest1() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.JUMP), 0x0}
	vm.AbsIntCfgHarness(contract)
}

func absIntTest3() {
	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{ byte(vm.PUSH1), 0x1,
							byte(vm.PUSH1), 0x55,
							byte(vm.MLOAD),
							byte(vm.LT),
							byte(vm.PUSH1), 0x0, //jump destination
							byte(vm.JUMPI),
							byte(vm.STOP)}
	_ = vm.AbsIntCfgHarness(contract)
}


/////////////////////////////////////////////////////

type dummyAccount struct{}

func (dummyAccount) SubBalance(amount *big.Int)                          {}
func (dummyAccount) AddBalance(amount *big.Int)                          {}
func (dummyAccount) SetAddress(common.Address)                           {}
func (dummyAccount) Value() *big.Int                                     { return nil }
func (dummyAccount) SetBalance(*big.Int)                                 {}
func (dummyAccount) SetNonce(uint64)                                     {}
func (dummyAccount) Balance() *big.Int                                   { return nil }
func (dummyAccount) Address() common.Address                             { return common.Address{} }
func (dummyAccount) ReturnGas(*big.Int)                                  {}
func (dummyAccount) SetCode(common.Hash, []byte)                         {}
func (dummyAccount) ForEachStorage(cb func(key, value common.Hash) bool) {}

type dummyStatedb struct {
	state.IntraBlockState
}

func (*dummyStatedb) GetRefund() uint64 { return 1337 }



/*
func testGenCfg() error {
	env := vm.NewEVM(vm.Context{BlockNumber: big.NewInt(1)}, &dummyStatedb{}, params.TestChainConfig,
		vm.Config{
			EVMInterpreter: "SaInterpreter",
		}, nil)

	contract := vm.NewContract(dummyAccount{}, dummyAccount{}, uint256.NewInt(), 10000, vm.NewDestsCache(50000))
	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x1, 0x0}
	//contract.Code = []byte{byte(vm.ADD), 0x1, 0x1, 0x0}

	jt := newIstanbulInstructionSet()
	vm.ToCfg0(contract)
	//_, err := env.Interpreter().Run(contract, []byte{}, false)
	if err != nil {
		return err
	}

	print("Done")
	return nil
}*/