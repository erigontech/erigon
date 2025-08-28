// Copyright 2015 The go-ethereum Authors
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

package stages_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	protosentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestGenerateChain(t *testing.T) {
	t.Parallel()
	var (
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		key3, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		addr3   = crypto.PubkeyToAddress(key3.PublicKey)
	)

	h := log.Root().GetHandler()
	defer func() {
		log.Root().SetHandler(h)
	}()
	log.Root().SetHandler(log.DiscardHandler())

	// Ensure that key1 has some funds in the genesis block.
	gspec := &types.Genesis{
		Config: &chain.Config{HomesteadBlock: new(big.Int), ChainID: big.NewInt(1)},
		Alloc:  types.GenesisAlloc{addr1: {Balance: big.NewInt(1000000)}},
	}
	m := mock.MockWithGenesis(t, gspec, key1, false)

	// This call generates a chain of 5 blocks. The function runs for
	// each block and adds different features to gen based on the
	// block index.
	signer := types.LatestSignerForChainID(nil)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, addr1 sends addr2 some ether.
			tx, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr1), addr2, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, key1)
			gen.AddTx(tx)
		case 1:
			// In block 2, addr1 sends some more ether to addr2.
			// addr2 passes it on to addr3.
			tx1, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr1), addr2, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key1)
			tx2, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr3, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, key2)
			gen.AddTx(tx1)
			gen.AddTx(tx2)
		case 2:
			// Block 3 is empty but was mined by addr3.
			gen.SetCoinbase(addr3)
			gen.SetExtra([]byte("yeehaw"))
		case 3:
			// Block 4 includes blocks 2 and 3 as uncle headers (with modified extra data).
			b2 := gen.PrevBlock(1).Header()
			b2.Extra = []byte("foo")
			gen.AddUncle(b2)
			b3 := gen.PrevBlock(2).Header()
			b3.Extra = []byte("foo")
			gen.AddUncle(b3)
		}
	})
	if err != nil {
		fmt.Printf("generate chain: %v\n", err)
	}

	// Import the chain. This runs all block validation rules.
	if err := m.InsertChain(chain); err != nil {
		fmt.Printf("insert error%v\n", err)
		return
	}

	tx, err := m.DB.BeginTemporalRw(m.Ctx)
	if err != nil {
		fmt.Printf("beginro error: %v\n", err)
		return
	}
	defer tx.Rollback()

	st := state.New(m.NewStateReader(tx))

	if big.NewInt(5).Cmp(current(m, tx).Number()) != 0 {
		t.Errorf("wrong block number: %d", current(m, tx).Number())
	}
	balance, err := st.GetBalance(addr1)
	if err != nil {
		t.Error(err)
	}
	if !uint256.NewInt(989000).Eq(&balance) {
		t.Errorf("wrong balance of addr1: %s", &balance)
	}
	balance, err = st.GetBalance(addr2)
	if err != nil {
		t.Error(err)
	}
	if !uint256.NewInt(10000).Eq(&balance) {
		t.Errorf("wrong balance of addr2: %s", &balance)
	}
	balance, err = st.GetBalance(addr3)
	if err != nil {
		t.Error(err)
	}
	if fmt.Sprintf("%s", &balance) != "19687500000000001000" { //nolint
		t.Errorf("wrong balance of addr3: %s", &balance)
	}

	// Test of receipts
	hashPacket := make([]common.Hash, 0, len(chain.Blocks))
	for _, block := range chain.Blocks {
		hashPacket = append(hashPacket, block.Hash())
	}

	b, err := rlp.EncodeToBytes(&eth.GetReceiptsPacket66{
		RequestId:         1,
		GetReceiptsPacket: hashPacket,
	})
	if err != nil {
		t.Fatal(err)
	}

	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&protosentry.InboundMessage{Id: protosentry.MessageId_GET_RECEIPTS_66, Data: b, PeerId: m.PeerId}) {
		if err != nil {
			t.Fatal(err)
		}
	}

	m.ReceiveWg.Wait()

	msg := m.SentMessage(0)

	if protosentry.MessageId_RECEIPTS_66 != msg.Id {
		t.Errorf("receipt id %d do not match the expected id %d", msg.Id, protosentry.MessageId_RECEIPTS_66)
	}
	r1 := types.Receipt{Type: 0, PostState: []byte{}, Status: 1, CumulativeGasUsed: 21000, Bloom: [256]byte{}, Logs: types.Logs{}, TxHash: common.HexToHash("0x9ca7a9e6bf23353fc5ac37f5c5676db1accec4af83477ac64cdcaa37f3a837f9"), ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"), GasUsed: 21000, BlockHash: common.HexToHash("0x5c7909bf8d4d8db71f0f6091aa412129591a8e41ff2230369ddf77a00bf57149"), BlockNumber: big.NewInt(1), TransactionIndex: 0}
	r2 := types.Receipt{Type: 0, PostState: []byte{}, Status: 1, CumulativeGasUsed: 21000, Bloom: [256]byte{}, Logs: types.Logs{}, TxHash: common.HexToHash("0xf190eed1578cdcfe69badd05b7ef183397f336dc3de37baa4adbfb4bc657c11e"), ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"), GasUsed: 21000, BlockHash: common.HexToHash("0xe4d4617526870ba7c5b81900e31bd2525c02f27fe06fd6c3caf7bed05f3271f4"), BlockNumber: big.NewInt(2), TransactionIndex: 0}
	r3 := types.Receipt{Type: 0, PostState: []byte{}, Status: 1, CumulativeGasUsed: 42000, Bloom: [256]byte{}, Logs: types.Logs{}, TxHash: common.HexToHash("0x309a030e44058e435a2b01302006880953e2c9319009db97013eb130d7a24eab"), ContractAddress: common.HexToAddress("0x0000000000000000000000000000000000000000"), GasUsed: 21000, BlockHash: common.HexToHash("0xe4d4617526870ba7c5b81900e31bd2525c02f27fe06fd6c3caf7bed05f3271f4"), BlockNumber: big.NewInt(2), TransactionIndex: 1}

	encodedEmpty, err := rlp.EncodeToBytes(types.Receipts{})
	if err != nil {
		t.Fatal(err)
	}
	encodedFirst, err := rlp.EncodeToBytes(types.Receipts{
		&r1,
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedSecond, err := rlp.EncodeToBytes(types.Receipts{
		&r2,
		&r3,
	})
	if err != nil {
		t.Fatal(err)
	}

	res := []rlp.RawValue{
		encodedFirst,
		encodedSecond,
		encodedEmpty,
		encodedEmpty,
		encodedEmpty,
	}

	b, err = rlp.EncodeToBytes(&eth.ReceiptsRLPPacket66{
		RequestId:         1,
		ReceiptsRLPPacket: res,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(msg.GetData()) {
		t.Errorf("receipt data %s do not match the expected msg %s", string(msg.GetData()), string(b))
	}
	if string(b) != string(msg.GetData()) {
		t.Errorf("receipt data %s do not match the expected msg %s", string(msg.GetData()), string(b))
	}

}
