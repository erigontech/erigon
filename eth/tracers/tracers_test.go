// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tracers_test

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/json"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/require"

	"github.com/holiman/uint256"

	// Force-load native and js packages, to trigger registration
	"github.com/ledgerwatch/erigon/eth/tracers"
	_ "github.com/ledgerwatch/erigon/eth/tracers/js"
	_ "github.com/ledgerwatch/erigon/eth/tracers/native"
)

func TestPrestateTracerCreate2(t *testing.T) {
	unsignedTx := types.NewTransaction(1, libcommon.HexToAddress("0x00000000000000000000000000000000deadbeef"),
		uint256.NewInt(0), 5000000, uint256.NewInt(1), []byte{})

	privateKeyECDSA, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	signer := types.LatestSignerForChainID(big.NewInt(1))
	txn, err := types.SignTx(unsignedTx, *signer, privateKeyECDSA)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	/**
		This comes from one of the test-vectors on the Skinny Create2 - EIP

	    address 0x00000000000000000000000000000000deadbeef
	    salt 0x00000000000000000000000000000000000000000000000000000000cafebabe
	    init_code 0xdeadbeef
	    gas (assuming no mem expansion): 32006
	    result: 0x60f3f640a8508fC6a86d45DF051962668E1e8AC7
	*/
	origin, _ := signer.Sender(txn)
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: uint256.NewInt(1),
	}
	excessBlobGas := uint64(50000)
	context := evmtypes.BlockContext{
		CanTransfer:   core.CanTransfer,
		Transfer:      core.Transfer,
		Coinbase:      libcommon.Address{},
		BlockNumber:   8000000,
		Time:          5,
		Difficulty:    big.NewInt(0x30000),
		GasLimit:      uint64(6000000),
		ExcessBlobGas: &excessBlobGas,
	}
	context.BaseFee = uint256.NewInt(0)
	alloc := types.GenesisAlloc{}

	// The code pushes 'deadbeef' into memory, then the other params, and calls CREATE2, then returns
	// the address
	alloc[libcommon.HexToAddress("0x00000000000000000000000000000000deadbeef")] = types.GenesisAccount{
		Nonce:   1,
		Code:    hexutil.MustDecode("0x63deadbeef60005263cafebabe6004601c6000F560005260206000F3"),
		Balance: big.NewInt(1),
	}
	alloc[origin] = types.GenesisAccount{
		Nonce:   1,
		Code:    []byte{},
		Balance: big.NewInt(500000000000000),
	}

	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	rules := params.AllProtocolChanges.Rules(context.BlockNumber, context.Time)
	statedb, _ := tests.MakePreState(rules, tx, alloc, context.BlockNumber)

	// Create the tracer, the EVM environment and run it
	tracer, err := tracers.New("prestateTracer", new(tracers.Context), json.RawMessage("{}"))
	if err != nil {
		t.Fatalf("failed to prestate tracer: %v", err)
	}
	evm := vm.NewEVM(context, txContext, statedb, params.AllProtocolChanges, vm.Config{Debug: true, Tracer: tracer})

	msg, err := txn.AsMessage(*signer, nil, rules)
	if err != nil {
		t.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas()))
	if _, err = st.TransitionDb(false, false); err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	// Retrieve the trace result and compare against the etalon
	res, err := tracer.GetResult()
	if err != nil {
		t.Fatalf("failed to retrieve trace result: %v", err)
	}
	ret := make(map[string]interface{})
	if err := json.Unmarshal(res, &ret); err != nil {
		t.Fatalf("failed to unmarshal trace result: %v", err)
	}
	if _, has := ret["0x60f3f640a8508fc6a86d45df051962668e1e8ac7"]; !has {
		t.Fatalf("Expected 0x60f3f640a8508fc6a86d45df051962668e1e8ac7 in result")
	}
}
