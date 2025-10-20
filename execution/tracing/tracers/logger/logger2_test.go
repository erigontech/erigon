// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package logger

//func TestStoreCapture2(t *testing.T) {
//	var (
//		logger   = NewStructLogger(nil)
//		env      = vm.NewEVM(vm.BlockContext{}, vm.TxContext{}, &dummyStatedb{}, params.TestChainConfig, vm.Config{Debug: true, Tracer: logger})
//		contract = vm.NewContract(&dummyContractRef{}, &dummyContractRef{}, new(big.Int), 100000)
//	)
//	contract.Code = []byte{byte(vm.PUSH1), 0x1, byte(vm.PUSH1), 0x0, byte(vm.SSTORE)}
//	var index common.Hash
//	logger.CaptureStart(env, common.Address{}, contract.Address(), false, nil, 0, nil)
//	_, err := env.Interpreter().Run(contract, []byte{}, false)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(logger.storage[contract.Address()]) == 0 {
//		t.Fatalf("expected exactly 1 changed value on address %x, got %d", contract.Address(),
//			len(logger.storage[contract.Address()]))
//	}
//	exp := common.BigToHash(big.NewInt(1))
//	if logger.storage[contract.Address()][index] != exp {
//		t.Errorf("expected %x, got %x", exp, logger.storage[contract.Address()][index])
//	}
//}
