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

package executiontests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameTransactionInsertChain(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			previous := dbg.Exec3Parallel
			dbg.Exec3Parallel = parallel
			t.Cleanup(func() { dbg.Exec3Parallel = previous })
			cfg := chain.TestChainOsakaConfig.Copy()
			cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
			sender, target := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
			genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
				sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
				target: {Code: []byte{0x60, 1, 0x5f, 0x55, 0x5f, 0x5f, 0xa0}},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				for nonce := range uint64(2) {
					b.AddTx(&types.FrameTransaction{
						NonceKeys: []uint256.Int{{}},
						ChainID:   *cfg.ChainID, SenderAddress: sender, Nonce: nonce,
						Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}, {Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 200_000, State: genesis.GasLimit - params.StateGasPerStorageSet}}},
						Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000), TipCap: *uint256.NewInt(1_000_000_000)},
					})
				}
			})
			require.NoError(t, err)
			require.Len(t, pack.Receipts[0], 2)
			for _, receipt := range pack.Receipts[0] {
				require.Equal(t, &sender, receipt.Payer)
				require.Len(t, receipt.FrameReceipts, 2)
				require.Equal(t, types.ReceiptStatusSuccessful, receipt.FrameReceipts[1].Status)
				require.Len(t, receipt.Logs, 1)
				require.Len(t, receipt.FrameReceipts[1].Logs, 1)
				require.Equal(t, receipt.Logs[0], receipt.FrameReceipts[1].Logs[0])
			}
			require.NoError(t, m.InsertChain(pack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				ibs := state.New(m.NewStateReader(tx))
				defer ibs.Close()
				nonce, err := ibs.GetNonce(accounts.InternAddress(sender))
				require.NoError(t, err)
				require.Equal(t, uint64(2), nonce)
				value, err := ibs.GetState(accounts.InternAddress(target), accounts.ZeroKey)
				require.NoError(t, err)
				require.Equal(t, uint256.NewInt(1), &value)
				return nil
			}))
		})
	}
}

func TestFrameValidationUsesPublishedHead(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender, withdrawn, recipient := common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	genesis := &types.Genesis{Config: cfg, Timestamp: uint64(time.Now().Unix()) + 60, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender:                    {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
		withdrawn:                 {Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
		recipient:                 {Balance: big.NewInt(1)},
		types.FrameNonceManager:   {Nonce: 1, Code: common.FromHex("60006000fd")},
		types.FrameExpiryVerifier: {Code: common.FromHex("60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd")},
	}}
	var armed atomic.Bool
	commitReady, proceed := make(chan struct{}), make(chan struct{})
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithStateTransitionObserver(func(ctx context.Context, point execmodule.StateTransitionPoint) {
			if point != execmodule.StateTransitionCommitReady || !armed.CompareAndSwap(true, false) {
				return
			}
			close(commitReady)
			select {
			case <-proceed:
			case <-ctx.Done():
			}
		}))
	frame := func(sender common.Address, key, nonce, fee uint64) *types.FrameTransaction {
		return &types.FrameTransaction{
			NonceKeys: []uint256.Int{*uint256.NewInt(key)},
			ChainID:   *cfg.ChainID, SenderAddress: sender, Nonce: nonce,
			Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000, State: params.StateGasPerStorageSet}}},
			Fees:   types.FrameFees{FeeCap: *uint256.NewInt(fee)},
		}
	}
	expiring := frame(sender, 2, 0, 10_000_000_000)
	expiring.Frames = append([]types.Frame{{Mode: types.FrameVerify, Target: &types.FrameExpiryVerifier, Data: make([]byte, 8), Limits: mdgas.MdGas{Execution: 5_000}}}, expiring.Frames...)
	binary.BigEndian.PutUint64(expiring.Frames[0].Data, m.Genesis.Time()+cfg.SecondsPerSlot())
	tests := []struct {
		name                     string
		tx                       *types.FrameTransaction
		validBefore, validAtHead bool
	}{
		{"withdrawal", frame(withdrawn, 0, 0, 10_000_000_000), false, true},
		{"consumed key", frame(sender, 1, 0, 10_000_000_000), true, false},
		{"next sequence", frame(sender, 1, 1, 10_000_000_000), false, true},
		{"other key", frame(sender, 2, 0, 10_000_000_000), true, true},
		{"reduced payer balance", frame(sender, 2, 0, 1_000_000_000_000), true, false},
		{"expired", expiring, true, false},
	}
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	check := func(phase string, atHead bool) {
		t.Run(phase, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					var encoded bytes.Buffer
					require.NoError(t, tt.tx.MarshalBinary(&encoded))
					result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
					require.NoError(t, err)
					valid := tt.validBefore
					if atHead {
						valid = tt.validAtHead
					}
					require.Equal(t, valid, result.Valid)
					if tt.tx.SenderAddress == withdrawn && atHead {
						require.Equal(t, uint256.NewInt(1_000_000_000_000_000_000).Bytes(), result.PayerBalance)
					}
				})
			}
		})
	}
	check("before publication", false)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.AddWithdrawal(&types.Withdrawal{Address: withdrawn, Amount: 1_000_000_000})
		tx := frame(sender, 1, 0, 10_000_000_000)
		tx.Frames = append(tx.Frames, types.Frame{Mode: types.FrameSender, Target: &recipient, Value: *uint256.NewInt(900_000_000_000_000_000), Limits: mdgas.MdGas{Execution: 50_000}})
		b.AddTx(tx)
	})
	require.NoError(t, err)
	status, err := m.InsertBlocks(t.Context(), pack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	armed.Store(true)
	fcuDone := make(chan error, 1)
	go func() {
		_, err := m.UpdateForkChoice(t.Context(), pack.TopBlock.Header())
		fcuDone <- err
	}()
	release := sync.OnceFunc(func() { close(proceed) })
	defer func() {
		release()
		require.NoError(t, <-fcuDone)
		m.ExecModule.WaitIdle(t.Context())
	}()
	select {
	case <-commitReady:
	case <-time.After(time.Minute):
		t.Fatal("forkchoice did not reach commit")
	}
	require.Equal(t, uint64(0), m.Current(nil).NumberU64())
	check("before commit", true)
	db := &frameValidationCommitDB{TemporalRwDB: m.DB, afterOpen: sync.OnceFunc(func() {
		release()
		m.ExecModule.WaitIdle(t.Context())
	})}
	server = privateapi.NewEthBackendServer(t.Context(), nil, db, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	check("commit during validation", true)
	check("after commit", true)
}

func TestFrameValidationCreationNonces(t *testing.T) {
	for _, creation := range []vm.OpCode{vm.CREATE, vm.CREATE2} {
		t.Run(creation.String(), func(t *testing.T) {
			cfg := chain.TestChainOsakaConfig.Copy()
			cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
			factory := common.HexToAddress("0x2000")
			initcode := program.New().ReturnData(program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()).Bytes()
			sender := types.CreateAddress(factory, 1)
			code := program.New().Mstore(initcode, 0)
			want := &remoteproto.FrameCreationNonce{Address: factory[:], MinNonce: 1, MaxNonce: 1}
			if creation == vm.CREATE2 {
				sender = types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initcode)))
				code.Push0()
				want.MinNonce, want.MaxNonce = 0, math.MaxUint64-1
			}
			code.Push(len(initcode)).Push0().Push0().Op(creation, vm.POP, vm.STOP)
			genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
				sender:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
				factory: {Nonce: 1, Code: code.Bytes()},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			frame := &types.FrameTransaction{
				NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
				Frames: []types.Frame{
					{Mode: types.FrameDefault, Target: &factory, Limits: mdgas.MdGas{Execution: 80_000, State: 400_000}},
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				},
				Fees: types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
			}
			var encoded bytes.Buffer
			require.NoError(t, frame.MarshalBinary(&encoded))
			server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
			result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
			require.NoError(t, err)
			require.True(t, result.Valid)
			require.Equal(t, []*remoteproto.FrameCreationNonce{want}, result.CreationNonces)
		})
	}
}

func TestFrameValidationPendingFees(t *testing.T) {
	for _, fee := range []uint64{90, 10} {
		t.Run(fmt.Sprint(fee), func(t *testing.T) {
			cfg := chain.TestChainOsakaConfig.Copy()
			cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
			sender := common.HexToAddress("0x1000")
			genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(100), Alloc: types.GenesisAlloc{
				sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			frame := &types.FrameTransaction{
				NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
				Fees:   types.FrameFees{FeeCap: *uint256.NewInt(fee)},
			}
			var encoded bytes.Buffer
			require.NoError(t, frame.MarshalBinary(&encoded))
			server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
			result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
			require.NoError(t, err)
			require.True(t, result.Valid, "pool fee queues must decide when a validated transaction can be included")
		})
	}
}

func TestFrameValidationAtBogotaActivation(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	now := uint64(time.Now().Unix())
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(now-1)
	sender := common.HexToAddress("0x1000")
	genesis := &types.Genesis{Config: cfg, Timestamp: now - 2*cfg.SecondsPerSlot(), GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	frame := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
		Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
	}
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
	require.NoError(t, err)
	require.True(t, result.Valid, "admission must use active fork rules before the first post-fork block")
}

func TestFrameValidationExpectedHead(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	frame := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
		Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
	}
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	for _, hash := range []common.Hash{m.Genesis.Hash(), {1}} {
		result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{
			Transaction: encoded.Bytes(), ExpectedBlockHash: gointerfaces.ConvertHashToH256(hash),
		})
		if hash != m.Genesis.Hash() {
			require.Equal(t, codes.Aborted, status.Code(err), "head disagreement must be retryable")
			continue
		}
		require.NoError(t, err)
		require.True(t, result.Valid)
	}
}

func TestFrameValidationRejectsBlockhash(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	code := program.New().Push0().Op(vm.BLOCKHASH, vm.POP).Push(3).Push0().Push0().Op(vm.APPROVE).Bytes()
	genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: code},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	pack, err := m.GenerateChain(2, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	frame := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
		Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
	}
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
	require.NoError(t, err)
	require.False(t, result.Valid)
}

func TestFrameValidationPendingBlobFee(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	genesis := &types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(100), ExcessBlobGas: common.NewUint64(100_000_000),
		Alloc: types.GenesisAlloc{sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	frame := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
		Frames:              []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
		Fees:                types.FrameFees{FeeCap: *uint256.NewInt(100), BlobFeeCap: *uint256.NewInt(1)},
		BlobVersionedHashes: []common.Hash{{1}},
	}
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
	require.NoError(t, err)
	require.True(t, result.Valid, "a low blob fee cap must not invalidate the prefix")
}

type frameValidationCommitDB struct {
	kv.TemporalRwDB
	afterOpen func()
}

func (db *frameValidationCommitDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := db.TemporalRwDB.BeginTemporalRo(ctx) //nolint:gocritic // The caller owns the returned transaction.
	if err == nil {
		db.afterOpen()
	}
	return tx, err
}
