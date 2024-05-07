package core_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func TestEIP3074(t *testing.T) {
	var (
		aa     = libcommon.HexToAddress("0x000000000000000000000000000000000000aaaa")
		bb     = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
		engine = ethash.NewFaker()

		// A sender who makes transactions, has some funds
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		funds  = new(big.Int).Mul(libcommon.Big1, big.NewInt(params.Ether))
		gspec  = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc: types.GenesisAlloc{
				addr: {Balance: funds},
				// The address 0xAAAA sloads 0x00 and 0x01
				aa: {
					Code:    nil, // added below
					Nonce:   0,
					Balance: big.NewInt(0),
				},
				// The address 0xBBBB calls 0xAAAA
				bb: {
					Code: []byte{
						byte(vm.CALLER),
						byte(vm.PUSH0),
						byte(vm.SSTORE),
						byte(vm.STOP),
					},
					Nonce:   0,
					Balance: big.NewInt(0),
				},
			},
		}
	)

	invoker := []byte{
		// copy sig to memory
		byte(vm.CALLDATASIZE),
		byte(vm.PUSH0),
		byte(vm.PUSH0),
		byte(vm.CALLDATACOPY),

		// set up auth
		byte(vm.CALLDATASIZE),
		byte(vm.PUSH0),
	}
	// push authority to stack
	invoker = append(invoker, append([]byte{byte(vm.PUSH20)}, addr.Bytes()...)...)
	invoker = append(invoker, []byte{

		byte(vm.AUTH),
		byte(vm.POP),

		// execute authcall
		byte(vm.PUSH0), // out size
		byte(vm.DUP1),  // out offset
		byte(vm.DUP1),  // out insize
		byte(vm.DUP1),  // in offset
		byte(vm.DUP1),  // valueExt
		byte(vm.DUP1),  // value
		byte(vm.PUSH2), // address
		byte(0xbb),
		byte(0xbb),
		byte(vm.PUSH0),
		byte(vm.AUTHCALL),
		byte(vm.STOP),
	}...,
	)

	// Set the invoker's code.
	if entry := gspec.Alloc[aa]; true {
		entry.Code = invoker
		gspec.Alloc[aa] = entry
	}

	gspec.Config.LondonBlock = libcommon.Big0
	gspec.Config.PragueTime = big.NewInt(0)
	signer := types.LatestSigner(gspec.Config)

	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, gspec, engine, false, checkStateRoot)
	//logger := logger.NewMarkdownLogger(&logger.LogConfig{}, os.Stderr)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		commit := libcommon.Hash{0x42}
		msg := []byte{params.AuthMagicPrefix}
		msg = append(msg, common.LeftPadBytes(gspec.Config.ChainID.Bytes(), 32)...)
		msg = append(msg, common.LeftPadBytes(libcommon.Big1.Bytes(), 32)...)
		msg = append(msg, common.LeftPadBytes(aa.Bytes(), 32)...)
		msg = append(msg, commit.Bytes()...)
		msg = crypto.Keccak256(msg)

		sig, _ := crypto.Sign(msg, key)
		sig = append([]byte{sig[len(sig)-1]}, sig[0:len(sig)-1]...)
		txdata := &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce: 0,
				Gas:   500000,
				To:    &aa,
				Data:  append(sig, commit.Bytes()...),
			},
			ChainID:    uint256.MustFromBig(gspec.Config.ChainID),
			Tip:        uint256.NewInt(2),
			FeeCap:     uint256.NewInt(params.GWei * 5),
			AccessList: nil,
		}
		tx, _ := types.SignTx(txdata, *signer, key)
		b.AddTx(tx)
		//b.AddTxWithChainVMConfig(nil, nil, tx, vm.Config{Debug: true, Tracer: logger}) // keeping this for debugging
	})
	if err != nil {
		t.Fatalf("failed to generate chain: %v", err)
	}
	if err := m.InsertChain(chain); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	// Verify authcall worked correctly.
	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(m.NewStateReader(tx))
		if !st.Exist(bb) {
			t.Error("expected contractAddress to exist", bb.String())
		}

		got := uint256.NewInt(0)
		st.GetState(bb, &libcommon.Hash{}, got)

		if want := addr.Bytes(); !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("incorrect sender in authcall: got %s, want %s", got.Hex(), common.Bytes2Hex(want))
		}

		return nil
	})

	if err != nil {
		t.Fatalf("failed to verify authcall: %v", err)
	}
}
