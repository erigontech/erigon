// Copyright 2026 The Erigon Authors
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

package misc_test

import (
	"encoding/binary"
	"math/big"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	pmisc "github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// ethTransferTestCode is the bytecode of the Solidity contract below.
var ethTransferTestCode = common.FromHex("6080604052600436106100345760003560e01c8063574ffc311461003957806366e41cb714610090578063f8a8fd6d1461009a575b600080fd5b34801561004557600080fd5b5061004e6100a4565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b6100986100ac565b005b6100a26100f5565b005b63deadbeef81565b7f38e80b5c85ba49b7280ccc8f22548faa62ae30d5a008a1b168fba5f47f5d1ee560405160405180910390a1631234567873ffffffffffffffffffffffffffffffffffffffff16ff5b7f24ec1d3ff24c2f6ff210738839dbc339cd45a5294d85c79361016243157aae7b60405160405180910390a163deadbeef73ffffffffffffffffffffffffffffffffffffffff166002348161014657fe5b046040516024016040516020818303038152906040527f66e41cb7000000000000000000000000000000000000000000000000000000007bffffffffffffffffffffffffffffffffffffffffffffffffffffffff19166020820180517bffffffffffffffffffffffffffffffffffffffffffffffffffffffff83818316178352505050506040518082805190602001908083835b602083106101fd57805182526020820191506020810190506020830392506101da565b6001836020036101000a03801982511681845116808217855250505050505090500191505060006040518083038185875af1925050503d806000811461025f576040519150601f19603f3d011682016040523d82523d6000602084013e610264565b606091505b50505056fea265627a7a723158202cce817a434785d8560c200762f972d453ccd30694481be7545f9035a512826364736f6c63430005100032")

/*
pragma solidity >=0.4.22 <0.6.0;

contract TestLogs {

  address public constant target_contract = 0x00000000000000000000000000000000DeaDBeef;
  address payable constant selfdestruct_addr = 0x0000000000000000000000000000000012345678;

  event Response(bool success, bytes data);
    event TestEvent();
    event TestEvent2();

    function test() public payable {
       emit TestEvent();
        target_contract.call.value(msg.value/2)(abi.encodeWithSignature("test2()"));
    }
    function test2() public payable {
       emit TestEvent2();
       selfdestruct(selfdestruct_addr);
    }
}
*/

// TestEthTransferLogs tests EIP-7708 ETH transfer log output by simulating a
// scenario including transaction, CALL and SELFDESTRUCT value transfers, and
// also "ordinary" logs emitted. The same scenario is also tested with no value
// transferred.
func TestEthTransferLogs(t *testing.T) {
	testEthTransferLogs(t, 1_000_000_000)
	testEthTransferLogs(t, 0)
}

func testEthTransferLogs(t *testing.T, value uint64) {
	key1, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)

	addr1 := accounts.InternAddress(crypto.PubkeyToAddress(key1.PublicKey))
	addr2 := accounts.InternAddress(common.HexToAddress("cafebabe")) // caller
	addr3 := accounts.InternAddress(common.HexToAddress("deadbeef")) // callee
	addr4 := accounts.InternAddress(common.HexToAddress("12345678")) // selfdestruct target

	testEvent := crypto.Keccak256Hash([]byte("TestEvent()"))
	testEvent2 := crypto.Keccak256Hash([]byte("TestEvent2()"))

	// Start from a copy of AllProtocolChanges (to avoid mutating the global
	// and to keep the embedded sync.Once/noCopy fields intact) and ensure
	// Amsterdam is active from genesis time so EIP-7708 is in effect.
	var config chain.Config
	require.NoError(t, copier.CopyWithOption(&config, chain.AllProtocolChanges, copier.Option{DeepCopy: true}))
	if config.AmsterdamTime == nil {
		config.AmsterdamTime = big.NewInt(0)
	}

	gspec := &types.Genesis{
		Config:   &config,
		GasLimit: 10_000_000,
		Alloc: types.GenesisAlloc{
			addr1.Value(): {Balance: big.NewInt(1_000_000_000_000_000_000)},    // fund sender (1 ETH)
			addr2.Value(): {Balance: big.NewInt(0), Code: ethTransferTestCode}, // caller contract
			addr3.Value(): {Balance: big.NewInt(0), Code: ethTransferTestCode}, // callee contract
		},
	}

	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithChainConfig(&config),
		execmoduletester.WithKey(key1),
	)

	signer := types.LatestSigner(m.ChainConfig)

	chainPack, err := blockgen.GenerateChain(
		m.ChainConfig,
		m.Genesis,
		m.Engine,
		m.DB,
		1,
		func(i int, b *blockgen.BlockGen) {
			chainID, overflow := uint256.FromBig(m.ChainConfig.ChainID)
			require.False(t, overflow)

			var amount uint256.Int
			amount.SetUint64(value)

			feeCap := uint256.NewInt(5_000_000_000)
			tipCap := uint256.NewInt(5_000_000_000)

			tx := types.NewEIP1559Transaction(
				*chainID,
				0,
				addr2.Value(),
				&amount,
				500_000,
				feeCap,
				tipCap,
				feeCap,
				common.FromHex("f8a8fd6d"),
			)
			signedTx := types.MustSignNewTx(key1, *signer, tx)
			b.AddTx(signedTx)
		},
	)
	require.NoError(t, err)

	receipts := chainPack.Receipts[0]
	if len(receipts) != 1 {
		t.Fatalf("expected 1 receipt, got %d", len(receipts))
	}
	logs := receipts[0].Logs

	addrHash := func(addr common.Address) (hash common.Hash) {
		copy(hash[12:], addr[:])
		return
	}
	u256 := func(amount uint64) []byte {
		data := make([]byte, 32)
		binary.BigEndian.PutUint64(data[24:], amount)
		return data
	}

	var expLogs []*types.Log
	if value != 0 {
		expLogs = []*types.Log{
			{
				Address: params.SystemAddress.Value(),
				Topics: []common.Hash{
					pmisc.EthTransferLogEvent,
					addrHash(addr1.Value()),
					addrHash(addr2.Value()),
				},
				Data: u256(value),
			},
			{
				Address: addr2.Value(),
				Topics:  []common.Hash{testEvent},
				Data:    nil,
			},
			{
				Address: params.SystemAddress.Value(),
				Topics: []common.Hash{
					pmisc.EthTransferLogEvent,
					addrHash(addr2.Value()),
					addrHash(addr3.Value()),
				},
				Data: u256(value / 2),
			},
			{
				Address: addr3.Value(),
				Topics:  []common.Hash{testEvent2},
				Data:    nil,
			},
			{
				Address: params.SystemAddress.Value(),
				Topics: []common.Hash{
					pmisc.EthTransferLogEvent,
					addrHash(addr3.Value()),
					addrHash(addr4.Value()),
				},
				Data: u256(value / 2),
			},
		}
	} else {
		expLogs = []*types.Log{
			{
				Address: addr2.Value(),
				Topics:  []common.Hash{testEvent},
				Data:    nil,
			},
			{
				Address: addr3.Value(),
				Topics:  []common.Hash{testEvent2},
				Data:    nil,
			},
		}
	}

	if len(expLogs) != len(logs) {
		t.Fatalf("Incorrect number of logs (expected: %d, got: %d)", len(expLogs), len(logs))
	}
	for i := range expLogs {
		if !reflect.DeepEqual(expLogs[i].Address, logs[i].Address) ||
			!reflect.DeepEqual(expLogs[i].Topics, logs[i].Topics) ||
			!reflect.DeepEqual(expLogs[i].Data, logs[i].Data) {
			t.Fatalf("Incorrect log at index %d (expected: %#v, got: %#v)", i, expLogs[i], logs[i])
		}
	}
}
