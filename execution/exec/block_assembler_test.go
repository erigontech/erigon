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

package exec

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// TestBlockAssemblerMinTxGasEarlyExit pins the early-exit threshold in
// AddTransactions either side of Amsterdam, and pins it to the execution
// dimension alone. Every case uses a zero-value self-transfer so its intrinsic
// gas is exactly the fork minimum.
func TestBlockAssemblerMinTxGasEarlyExit(t *testing.T) {
	t.Parallel()

	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privateKey.PublicKey)
	senderAcc := accounts.InternAddress(sender)
	engine := merge.NewFaker(ethash.NewFaker())

	makeConfig := func(amsterdam bool) *chain.Config {
		cfg := chain.AllProtocolChanges.Copy()
		if amsterdam {
			cfg.AmsterdamTime = common.NewUint64(0)
		} else {
			cfg.AmsterdamTime = nil
		}
		return cfg
	}

	const blockGasLimit = 100_000

	tests := []struct {
		name          string
		amsterdam     bool
		gasUsed       uint64 // execution gas already consumed; remaining = blockGasLimit - gasUsed
		stateGasUsed  uint64 // state gas already consumed
		wantEarlyExit bool
		wantPacked    int
	}{
		// Pre-Amsterdam: threshold is params.TxGas (21,000).
		{"pre-Amsterdam/exec=20999/exits", false, 79_001, 0, true, 0},
		{"pre-Amsterdam/exec=21000/continues", false, 79_000, 0, false, 1},
		// Paired with post-Amsterdam/exec=15000: same gas, different fork,
		// different outcome.
		{"pre-Amsterdam/exec=15000/exits", false, 85_000, 0, true, 0},
		// The state dimension is never consumed pre-Amsterdam.
		{"pre-Amsterdam/stateExhausted/continues", false, 79_000, 88_001, false, 1},

		// Post-Amsterdam: threshold is params.TxBaseEIP2780 (12,000).
		{"post-Amsterdam/exec=11999/exits", true, 88_001, 0, true, 0},
		{"post-Amsterdam/exec=12000/continues", true, 88_000, 0, false, 1},
		{"post-Amsterdam/exec=15000/continues", true, 85_000, 0, false, 1},
		// Exec plentiful, state exhausted: the scan continues and this
		// transaction is rejected on its own state-gas contribution.
		{"post-Amsterdam/stateExhausted/noEarlyExit", true, 50_000, 88_001, false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chainCfg := makeConfig(tc.amsterdam)

			var txGasLimit uint64
			if tc.amsterdam {
				txGasLimit = params.TxBaseEIP2780
			} else {
				txGasLimit = params.TxGas
			}

			header := &types.Header{
				Number:   *uint256.NewInt(1),
				Time:     1000,
				GasLimit: blockGasLimit,
			}
			block := &AssembledBlock{Header: header}
			ba := NewBlockAssembler(AssemblerCfg{ChainConfig: chainCfg, Engine: engine}, block)
			ba.gasUsed.BlockExecution = tc.gasUsed
			ba.gasUsed.BlockState = tc.stateGasUsed

			ibs := state.New(state.NewNoopReader())
			require.NoError(t, ibs.AddBalance(senderAcc, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified))

			signer := types.MakeSigner(chainCfg, 1, 1000)
			to := sender
			tx, err := types.SignNewTx(privateKey, *signer, &types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    0,
					To:       &to,
					GasLimit: txGasLimit,
				},
			})
			require.NoError(t, err)

			logs, done, err := ba.AddTransactions(
				context.Background(),
				nil,
				types.Transactions{tx},
				accounts.NilAddress,
				&vm.Config{NoBaseFee: true},
				ibs,
				nil,
				"test",
				log.Root(),
			)
			require.NoError(t, err)

			if tc.wantEarlyExit {
				require.True(t, done, "expected early exit (done=true)")
				require.Empty(t, logs, "no logs on early exit")
			} else {
				require.False(t, done, "expected loop to continue past gas check")
			}
			require.Len(t, ba.Txns, tc.wantPacked)
		})
	}
}

// A versioned (Amsterdam) block assembler must remove a touched empty account
// before the next tx: a later EIP-7702 authorization by that account must cost
// the same as for an absent one.
func TestBlockAssemblerRemovesTouchedEmptyAccountBeforeNextTx(t *testing.T) {
	touchKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	setCodeKey, err := crypto.HexToECDSA("9a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	senders := []common.Address{crypto.PubkeyToAddress(touchKey.PublicKey), crypto.PubkeyToAddress(setCodeKey.PublicKey)}
	authorityKey, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)

	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	signer := types.LatestSignerForChainID(config.ChainID)
	gasPrice := uint256.NewInt(10_000_000_000)
	auth, err := types.SignAuthorization(authorityKey, *config.ChainID, common.HexToAddress("0xcafe"), 0)
	require.NoError(t, err)
	to := common.HexToAddress("0xbeef")

	setCodeGasUsed := func(withEmptyAuthority bool) uint64 {
		ctx := context.Background()
		logger := log.New()
		db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
		require.NoError(t, db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
			domains, err := execctx.NewSharedDomains(ctx, tx, logger)
			if err != nil {
				return err
			}
			defer domains.Close()
			domains.SetTxNum(1)
			put := func(addr common.Address, acc accounts.Account) error {
				return domains.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), 1, nil)
			}
			for _, sender := range senders {
				acc := accounts.NewAccount()
				acc.Balance = *uint256.NewInt(1_000_000_000_000_000_000)
				if err := put(sender, acc); err != nil {
					return err
				}
			}
			if withEmptyAuthority {
				if err := put(authority, accounts.NewAccount()); err != nil {
					return err
				}
			}
			if err := rawdbv3.TxNums.Append(tx, 0, 1); err != nil {
				return err
			}
			return domains.Flush(ctx, tx)
		}))

		tx, err := db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		domains, err := execctx.NewSharedDomains(ctx, tx, logger)
		require.NoError(t, err)
		defer domains.Close()
		domains.SetTxNum(2)
		ibs := state.New(state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
		defer ibs.Close()
		ibs.SetVersionMap(state.NewVersionMap(nil))
		ibs.SetTxContext(1, -1)

		header := types.NewEmptyHeaderForAssembling()
		header.Coinbase = common.Address{1}
		header.Number = *uint256.NewInt(1)
		header.GasLimit = 30_000_000
		header.Time = 12
		header.BaseFee = uint256.NewInt(1_000_000_000)
		excessBlobGas := uint64(0)
		header.ExcessBlobGas = &excessBlobGas
		header.BlobGasUsed = new(uint64)
		header.ParentBeaconBlockRoot = &common.Hash{}
		slot := uint64(1)
		header.SlotNumber = &slot

		engine := merge.New(ethash.NewFaker())
		defer engine.Close()
		ba := NewBlockAssembler(AssemblerCfg{ChainConfig: config, Engine: engine}, &AssembledBlock{
			Header: header, Uncles: []*types.Header{}, Txns: types.Transactions{}, Receipts: types.Receipts{}, Withdrawals: []*types.Withdrawal{},
		})
		require.True(t, ba.HasBAL())
		require.NoError(t, ba.Initialize(ibs, tx, logger))

		touch, err := types.SignTx(types.NewTransaction(0, authority, uint256.NewInt(0), 100_000, gasPrice, nil), *signer, touchKey)
		require.NoError(t, err)
		setCode, err := types.SignTx(&types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: 0, GasLimit: 500_000, To: &to},
				ChainID:  *config.ChainID,
				TipCap:   *gasPrice,
				FeeCap:   *gasPrice,
			},
			Authorizations: []types.Authorization{auth},
		}, *signer, setCodeKey)
		require.NoError(t, err)
		_, _, err = ba.AddTransactions(ctx, nil, types.Transactions{touch, setCode}, accounts.InternAddress(header.Coinbase), &vm.Config{}, ibs, nil, "test", logger)
		require.NoError(t, err)
		require.Len(t, ba.Receipts, 2)
		return ba.Receipts[1].GasUsed
	}
	require.Equal(t, setCodeGasUsed(false), setCodeGasUsed(true))
}

func TestAvailableRlpSpaceCoversFinalizedHeader(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		amsterdam bool
	}{
		{name: "osaka", amsterdam: false},
		{name: "amsterdam", amsterdam: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := chain.AllProtocolChanges.Copy()
			if !tc.amsterdam {
				cfg.AmsterdamTime = nil
			}
			beaconRoot := common.Hash{1}
			header := &types.Header{
				Number:                *uint256.NewInt(1),
				Time:                  1,
				GasLimit:              60_000_000,
				BaseFee:               uint256.NewInt(params.InitialBaseFee),
				BlobGasUsed:           new(uint64),
				ExcessBlobGas:         new(uint64),
				ParentBeaconBlockRoot: &beaconRoot,
			}
			if tc.amsterdam {
				header.SlotNumber = new(uint64)
			}
			mb := &AssembledBlock{Header: header, Withdrawals: []*types.Withdrawal{}}

			space := mb.AvailableRlpSpace(cfg)
			var txn types.Transaction
			for dataLen := space - 200; dataLen < space; dataLen++ {
				candidate := types.NewTransaction(0, common.Address{2}, uint256.NewInt(0), 50_000_000, uint256.NewInt(params.InitialBaseFee), make([]byte, dataLen))
				if post := mb.AvailableRlpSpace(cfg, candidate); post >= 0 && post < 8 {
					txn = candidate
					break
				}
			}
			require.NotNil(t, txn)
			require.GreaterOrEqual(t, mb.AvailableRlpSpace(cfg, txn), 0)
			mb.AddTxn(txn)

			header.GasUsed = 59_999_999
			*header.BlobGasUsed = cfg.GetMaxBlobGasPerBlock(header.Time)
			requestsHash := common.Hash{3}
			header.RequestsHash = &requestsHash
			if tc.amsterdam {
				balHash := common.Hash{4}
				header.BlockAccessListHash = &balHash
			}
			block := types.NewBlock(header, mb.Txns, nil, nil, mb.Withdrawals, nil)
			rawBlock := types.RawBlock{Header: block.Header(), Body: block.RawBody()}
			require.NoError(t, rawBlock.ValidateMaxRlpSize(cfg))
		})
	}
}
