// EVMC: Ethereum Client-VM Connector API.
// Copyright 2018-2019 The EVMC Authors.
// Licensed under the Apache License, Version 2.0.

package evmc

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

type testHostContext struct{}

func (host *testHostContext) AccountExists(addr common.Address) bool {
	return false
}

func (host *testHostContext) GetStorage(addr common.Address, key common.Hash) common.Hash {
	return common.Hash{}
}

func (host *testHostContext) SetStorage(addr common.Address, key common.Hash, value common.Hash) (status StorageStatus) {
	return StorageUnchanged
}

func (host *testHostContext) GetBalance(addr common.Address) common.Hash {
	return common.Hash{}
}

func (host *testHostContext) GetCodeSize(addr common.Address) int {
	return 0
}

func (host *testHostContext) GetCodeHash(addr common.Address) common.Hash {
	return common.Hash{}
}

func (host *testHostContext) GetCode(addr common.Address) []byte {
	return nil
}

func (host *testHostContext) Selfdestruct(addr common.Address, beneficiary common.Address) {
}

func (host *testHostContext) GetTxContext() TxContext {
	txContext := TxContext{}
	txContext.Number = 42
	return txContext
}

func (host *testHostContext) GetBlockHash(number int64) common.Hash {
	return common.Hash{}
}

func (host *testHostContext) EmitLog(addr common.Address, topics []common.Hash, data []byte) {
}

func (host *testHostContext) Call(kind CallKind,
	destination common.Address, sender common.Address, value *big.Int, input []byte, gas int64, depth int,
	static bool, salt *big.Int) (output []byte, gasLeft int64, createAddr common.Address, err error) {
	output = []byte("output from testHostContext.Call()")
	return output, gas, common.Address{}, nil
}

func TestGetTxContext(t *testing.T) {
	vm, _ := Load(modulePath)
	defer vm.Destroy()

	host := &testHostContext{}
	code := []byte("\x43\x60\x00\x52\x59\x60\x00\xf3")

	addr := common.Address{}
	h := common.Hash{}
	output, gasLeft, err := vm.Execute(host, Byzantium, Call, false, 1, 100, addr, addr, nil, h, code, h)

	if len(output) != 20 {
		t.Errorf("unexpected output size: %d", len(output))
	}
	if bytes.Compare(output[0:3], []byte("42\x00")) != 0 {
		t.Errorf("execution unexpected output: %s", output)
	}
	if gasLeft != 50 {
		t.Errorf("execution gas left is incorrect: %d", gasLeft)
	}
	if err != nil {
		t.Error("execution returned unexpected error")
	}
}

func TestCall(t *testing.T) {
	vm, _ := Load(modulePath)
	defer vm.Destroy()

	host := &testHostContext{}
	code := []byte("\x60\x00\x80\x80\x80\x80\x80\x80\xf1")

	addr := common.Address{}
	h := common.Hash{}
	output, gasLeft, err := vm.Execute(host, Byzantium, Call, false, 1, 100, addr, addr, nil, h, code, h)

	if bytes.Compare(output, []byte("output from testHostContext.Call()")) != 0 {
		t.Errorf("execution unexpected output: %s", output)
	}
	if gasLeft != 99 {
		t.Errorf("execution gas left is incorrect: %d", gasLeft)
	}
	if err != nil {
		t.Error("execution returned unexpected error")
	}
}
