// Copyright 2024 The Erigon Authors
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

package stagedsynctest

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func InitHarness(ctx context.Context, t *testing.T, cfg HarnessCfg) Harness {
	logger := testlog.Logger(t, cfg.LogLvl)
	genesisInit := createGenesisInitData(t, cfg.ChainConfig)
	m := mock.MockWithGenesis(t, genesisInit.genesis, genesisInit.genesisAllocPrivateKey, false)
	chainDataDB := m.DB
	blockReader := m.BlockReader
	borConsensusDB := memdb.NewTestDB(t, kv.ChainDB)
	ctrl := gomock.NewController(t)
	heimdallClient := heimdall.NewMockClient(ctrl)
	miningState := stagedsync.NewMiningState(&ethconfig.Defaults.Miner)

	stateSyncStages := stagedsync.DefaultStages(ctx, stagedsync.SnapshotsCfg{}, stagedsync.HeadersCfg{}, stagedsync.BlockHashesCfg{}, stagedsync.BodiesCfg{}, stagedsync.SendersCfg{}, stagedsync.ExecuteBlockCfg{}, stagedsync.TxLookupCfg{}, stagedsync.FinishCfg{}, true)
	stateSync := stagedsync.New(
		ethconfig.Defaults.Sync,
		stateSyncStages,
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
		logger,
		stages.ModeApplyingBlocks,
	)
	miningSyncStages := stagedsync.MiningStages(
		ctx,
		stagedsync.MiningCreateBlockCfg{},
		stagedsync.ExecuteBlockCfg{},
		stagedsync.SendersCfg{},
		stagedsync.MiningExecCfg{},
		stagedsync.MiningFinishCfg{},
		false,
	)
	miningSync := stagedsync.New(
		ethconfig.Defaults.Sync,
		miningSyncStages,
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
		logger,
		stages.ModeBlockProduction,
	)
	validatorKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	validatorAddress := crypto.PubkeyToAddress(validatorKey.PublicKey)
	h := Harness{
		logger:                    logger,
		chainDataDB:               chainDataDB,
		borConsensusDB:            borConsensusDB,
		chainConfig:               cfg.ChainConfig,
		borConfig:                 cfg.ChainConfig.Bor.(*borcfg.BorConfig),
		blockReader:               blockReader,
		stateSyncStages:           stateSyncStages,
		stateSync:                 stateSync,
		miningSyncStages:          miningSyncStages,
		miningSync:                miningSync,
		miningState:               miningState,
		heimdallClient:            heimdallClient,
		heimdallProducersOverride: cfg.GetOrCreateDefaultHeimdallProducersOverride(),
		sealedHeaders:             make(map[uint64]*types.Header),
		borSpanner:                bor.NewMockSpanner(ctrl),
		validatorAddress:          validatorAddress,
		validatorKey:              validatorKey,
		genesisInitData:           genesisInit,
	}

	if cfg.ChainConfig.Bor != nil {
		h.setHeimdallNextMockSpan()
		h.mockBorSpanner()
		h.mockHeimdallClient()
	}

	h.generateChain(ctx, t, ctrl, cfg)

	return h
}

type genesisInitData struct {
	genesis                 *types.Genesis
	genesisAllocPrivateKey  *ecdsa.PrivateKey
	genesisAllocPrivateKeys map[common.Address]*ecdsa.PrivateKey
	fundedAddresses         []common.Address
}

type HarnessCfg struct {
	ChainConfig               *chain.Config
	GenerateChainNumBlocks    int
	LogLvl                    log.Lvl
	HeimdallProducersOverride map[uint64][]valset.Validator
}

func (hc *HarnessCfg) GetOrCreateDefaultHeimdallProducersOverride() map[uint64][]valset.Validator {
	if hc.HeimdallProducersOverride == nil {
		hc.HeimdallProducersOverride = map[uint64][]valset.Validator{}
	}

	return hc.HeimdallProducersOverride
}

type Harness struct {
	logger                     log.Logger
	chainDataDB                kv.TemporalRwDB
	borConsensusDB             kv.RwDB
	chainConfig                *chain.Config
	borConfig                  *borcfg.BorConfig
	blockReader                interfaces.BlockReader
	stateSyncStages            []*stagedsync.Stage
	stateSync                  *stagedsync.Sync
	miningSyncStages           []*stagedsync.Stage
	miningSync                 *stagedsync.Sync
	miningState                stagedsync.MiningState
	heimdallClient             *heimdall.MockClient
	heimdallNextMockSpan       *heimdall.Span
	heimdallLastEventID        uint64
	heimdallLastEventHeaderNum uint64
	heimdallProducersOverride  map[uint64][]valset.Validator // spanID -> selected producers override
	sealedHeaders              map[uint64]*types.Header
	borSpanner                 *bor.MockSpanner
	validatorAddress           common.Address
	validatorKey               *ecdsa.PrivateKey
	genesisInitData            *genesisInitData
}

func (h *Harness) Logger() log.Logger {
	return h.logger
}

func (h *Harness) BorConfig() *borcfg.BorConfig {
	return h.borConfig
}

func (h *Harness) SaveStageProgress(ctx context.Context, t *testing.T, stageID stages.SyncStage, progress uint64) {
	rwTx, err := h.chainDataDB.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	err = stages.SaveStageProgress(rwTx, stageID, progress)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)
}

func (h *Harness) GetStageProgress(ctx context.Context, t *testing.T, stageID stages.SyncStage) uint64 {
	roTx, err := h.chainDataDB.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	progress, err := stages.GetStageProgress(roTx, stageID)
	require.NoError(t, err)
	return progress
}

func (h *Harness) StateSyncUnwindPoint() uint64 {
	return h.stateSync.UnwindPoint()
}

func (h *Harness) StateSyncUnwindReason() stagedsync.UnwindReason {
	return h.stateSync.UnwindReason()
}

func (h *Harness) RunStateSyncStageForward(t *testing.T, id stages.SyncStage) {
	h.RunStateSyncStageForwardWithErrorIs(t, id, nil)
}

func (h *Harness) RunStateSyncStageForwardWithErrorIs(t *testing.T, id stages.SyncStage, wantErr error) {
	h.runSyncStageForwardWithErrorIs(t, id, h.stateSync, h.stateSyncStages, wantErr, wrap.NewTxContainer(nil, nil))
}

func (h *Harness) RunStateStageForwardWithReturnError(t *testing.T, id stages.SyncStage) error {
	return h.runSyncStageForwardWithReturnError(t, id, h.stateSync, h.stateSyncStages, wrap.NewTxContainer(nil, nil))
}

func (h *Harness) RunMiningStageForward(ctx context.Context, t *testing.T, id stages.SyncStage) {
	h.RunMiningStageForwardWithErrorIs(ctx, t, id, nil)
}

func (h *Harness) RunMiningStageForwardWithErrorIs(ctx context.Context, t *testing.T, id stages.SyncStage, wantErr error) {
	tx, err := h.chainDataDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	txc := wrap.NewTxContainer(tx, nil)
	h.runSyncStageForwardWithErrorIs(t, id, h.miningSync, h.miningSyncStages, wantErr, txc)

	err = tx.Commit()
	require.NoError(t, err)
}

func (h *Harness) RunMiningStageForwardWithReturnError(ctx context.Context, t *testing.T, id stages.SyncStage) error {
	tx, err := h.chainDataDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	txc := wrap.NewTxContainer(tx, nil)
	err = h.runSyncStageForwardWithReturnError(t, id, h.miningSync, h.miningSyncStages, txc)
	if err != nil {
		return err
	}

	err = tx.Commit()
	require.NoError(t, err)
	return nil
}

func (h *Harness) SaveHeader(ctx context.Context, t *testing.T, header *types.Header) {
	h.saveHeaders(ctx, t, []*types.Header{header})
}

func (h *Harness) SetMiningBlockEmptyHeader(ctx context.Context, t *testing.T, parentNum uint64) {
	tx, err := h.chainDataDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	parent := rawdb.ReadHeaderByNumber(tx, parentNum)
	require.NotNil(t, parent)

	timestamp := uint64(time.Now().Unix())
	h.miningState.MiningBlock.Header = core.MakeEmptyHeader(parent, h.chainConfig, timestamp, h.miningState.MiningConfig.GasLimit)
}

func (h *Harness) ReadSpansFromDB(ctx context.Context) (spans []*heimdall.Span, err error) {
	err = h.chainDataDB.View(ctx, func(tx kv.Tx) error {
		spanIter, err := tx.Range(kv.BorSpans, nil, nil, order.Asc, kv.Unlim)
		if err != nil {
			return err
		}

		for spanIter.HasNext() {
			keyBytes, spanBytes, err := spanIter.Next()
			if err != nil {
				return err
			}

			spanKey := heimdall.SpanId(binary.BigEndian.Uint64(keyBytes))
			var heimdallSpan heimdall.Span
			if err = json.Unmarshal(spanBytes, &heimdallSpan); err != nil {
				return err
			}

			if spanKey != heimdallSpan.Id {
				return fmt.Errorf("span key and id mismatch %d!=%d", spanKey, heimdallSpan.Id)
			}

			spans = append(spans, &heimdallSpan)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return spans, nil
}

func (h *Harness) ReadStateSyncEventsFromDB(ctx context.Context) (eventIDs []uint64, err error) {
	err = h.chainDataDB.View(ctx, func(tx kv.Tx) error {
		eventsIter, err := tx.Range(kv.BorEvents, nil, nil, order.Asc, kv.Unlim)
		if err != nil {
			return err
		}

		for eventsIter.HasNext() {
			keyBytes, _, err := eventsIter.Next()
			if err != nil {
				return err
			}

			eventIDs = append(eventIDs, binary.BigEndian.Uint64(keyBytes))
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return eventIDs, nil
}

func (h *Harness) ReadLastStateSyncEventNumPerBlockFromDB(ctx context.Context) (nums map[uint64]uint64, err error) {
	nums = map[uint64]uint64{}
	err = h.chainDataDB.View(ctx, func(tx kv.Tx) error {
		eventNumsIter, err := tx.Range(kv.BorEventNums, nil, nil, order.Asc, kv.Unlim)
		if err != nil {
			return err
		}

		for eventNumsIter.HasNext() {
			blockNumBytes, lastEventNumBytes, err := eventNumsIter.Next()
			if err != nil {
				return err
			}

			blockNum := binary.BigEndian.Uint64(blockNumBytes)
			lastEventNum := binary.BigEndian.Uint64(lastEventNumBytes)
			nums[blockNum] = lastEventNum
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return nums, nil
}

func (h *Harness) ReadHeaderByNumber(ctx context.Context, number uint64) (header *types.Header, err error) {
	err = h.chainDataDB.View(ctx, func(tx kv.Tx) error {
		header = rawdb.ReadHeaderByNumber(tx, number)
		if header == nil {
			return errors.New("header not found by harness")
		}

		return nil
	})

	return
}

func createGenesisInitData(t *testing.T, chainConfig *chain.Config) *genesisInitData {
	t.Helper()
	accountPrivateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	accountAddress := crypto.PubkeyToAddress(accountPrivateKey.PublicKey)

	return &genesisInitData{
		genesisAllocPrivateKey: accountPrivateKey,
		genesis: &types.Genesis{
			Config: chainConfig,
			Alloc: types.GenesisAlloc{
				accountAddress: {
					Balance: new(big.Int).Exp(big.NewInt(1_000), big.NewInt(18), nil),
				},
			},
		},
		genesisAllocPrivateKeys: map[common.Address]*ecdsa.PrivateKey{
			accountAddress: accountPrivateKey,
		},
		fundedAddresses: []common.Address{
			accountAddress,
		},
	}
}

type dummySpanReader struct {
	consensus.ChainHeaderReader
}

// BorSpan mocks base method - this is required for pre-astrid testing
func (m dummySpanReader) BorSpan(arg0 uint64) *heimdall.Span {
	return nil
}

func (h *Harness) generateChain(ctx context.Context, t *testing.T, ctrl *gomock.Controller, cfg HarnessCfg) {
	if cfg.GenerateChainNumBlocks == 0 {
		return
	}

	consensusEngine := h.consensusEngine(t, cfg)
	var parentBlock *types.Block
	err := h.chainDataDB.View(ctx, func(tx kv.Tx) (err error) {
		parentBlock, err = h.blockReader.BlockByNumber(ctx, tx, 0)
		return err
	})
	require.NoError(t, err)
	h.sealedHeaders[parentBlock.Number().Uint64()] = parentBlock.Header()
	mockChainHR := dummySpanReader{h.mockChainHeaderReader(ctrl)}

	chainPack, err := core.GenerateChain(
		h.chainConfig,
		parentBlock,
		consensusEngine,
		h.chainDataDB,
		cfg.GenerateChainNumBlocks,
		func(i int, gen *core.BlockGen) {
			// seal parent block first so that we can Prepare the current header
			if gen.GetParent().Number().Uint64() > 0 {
				h.seal(t, mockChainHR, consensusEngine, gen.GetParent())
			}

			h.logger.Info("Preparing mock header", "headerNum", gen.GetHeader().Number)
			gen.GetHeader().ParentHash = h.sealedHeaders[gen.GetParent().Number().Uint64()].Hash()
			if err := consensusEngine.Prepare(mockChainHR, gen.GetHeader(), nil); err != nil {
				t.Fatal(err)
			}

			h.logger.Info("Adding 1 mock txn to block", "blockNum", gen.GetHeader().Number)
			chainID := uint256.Int{}
			overflow := chainID.SetFromBig(h.chainConfig.ChainID)
			require.False(t, overflow)
			from := h.genesisInitData.fundedAddresses[0]
			tx, err := types.SignTx(
				types.NewEIP1559Transaction(
					chainID,
					gen.TxNonce(from),
					from, // send to itself
					new(uint256.Int),
					21000,
					new(uint256.Int),
					new(uint256.Int),
					uint256.NewInt(937500001),
					nil,
				),
				*types.LatestSignerForChainID(h.chainConfig.ChainID),
				h.genesisInitData.genesisAllocPrivateKeys[from],
			)
			require.NoError(t, err)
			gen.AddTx(tx)
		},
	)
	require.NoError(t, err)

	h.seal(t, mockChainHR, consensusEngine, chainPack.TopBlock)
	sealedHeadersList := make([]*types.Header, len(h.sealedHeaders))
	for num, header := range h.sealedHeaders {
		sealedHeadersList[num] = header
	}

	h.saveHeaders(ctx, t, sealedHeadersList)
}

func (h *Harness) seal(t *testing.T, chr consensus.ChainHeaderReader, eng consensus.Engine, block *types.Block) {
	h.logger.Info("Sealing mock block", "blockNum", block.Number())
	sealRes, sealStop := make(chan *types.BlockWithReceipts, 1), make(chan struct{}, 1)
	if err := eng.Seal(chr, &types.BlockWithReceipts{Block: block}, sealRes, sealStop); err != nil {
		t.Fatal(err)
	}

	sealedParentBlock := <-sealRes
	h.sealedHeaders[sealedParentBlock.Block.Number().Uint64()] = sealedParentBlock.Block.Header()
}

func (h *Harness) consensusEngine(t *testing.T, cfg HarnessCfg) consensus.Engine {
	if h.chainConfig.Bor != nil {
		stateReceiver := bor.NewStateReceiver(h.borConfig.StateReceiverContractAddress())
		borConsensusEng := bor.New(
			h.chainConfig,
			h.borConsensusDB,
			nil,
			h.borSpanner,
			h.heimdallClient,
			stateReceiver,
			h.logger,
			nil,
			nil,
		)

		borConsensusEng.Authorize(h.validatorAddress, func(_ common.Address, _ string, msg []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(msg), h.validatorKey)
		})

		return borConsensusEng
	}

	t.Fatalf("unimplemented consensus engine init for cfg %v", cfg.ChainConfig)
	return nil
}

func (h *Harness) saveHeaders(ctx context.Context, t *testing.T, headers []*types.Header) {
	rwTx, err := h.chainDataDB.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	for _, header := range headers {
		err = rawdb.WriteHeader(rwTx, header)
		require.NoError(t, err)

		err = rawdb.WriteCanonicalHash(rwTx, header.Hash(), header.Number.Uint64())
		require.NoError(t, err)
	}

	err = rwTx.Commit()
	require.NoError(t, err)
}

func (h *Harness) mockChainHeaderReader(ctrl *gomock.Controller) consensus.ChainHeaderReader {
	mockChainHR := consensus.NewMockChainHeaderReader(ctrl)
	mockChainHR.
		EXPECT().
		GetHeader(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ common.Hash, number uint64) *types.Header {
			return h.sealedHeaders[number]
		}).
		AnyTimes()

	mockChainHR.
		EXPECT().
		GetHeaderByNumber(gomock.Any()).
		DoAndReturn(func(number uint64) *types.Header {
			return h.sealedHeaders[number]
		}).
		AnyTimes()

	mockChainHR.
		EXPECT().
		FrozenBlocks().
		Return(uint64(0)).
		AnyTimes()

	return mockChainHR
}

func (h *Harness) setHeimdallNextMockSpan() {
	validators := []*valset.Validator{
		{
			ID:               1,
			Address:          h.validatorAddress,
			VotingPower:      1000,
			ProposerPriority: 1,
		},
	}

	validatorSet := valset.NewValidatorSet(validators)
	selectedProducers := make([]valset.Validator, len(validators))
	for i := range validators {
		selectedProducers[i] = *validators[i]
	}

	h.heimdallNextMockSpan = &heimdall.Span{
		Id:                0,
		StartBlock:        0,
		EndBlock:          255,
		ValidatorSet:      *validatorSet,
		SelectedProducers: selectedProducers,
	}
}

func (h *Harness) mockBorSpanner() {
	h.borSpanner.
		EXPECT().
		GetCurrentValidators(gomock.Any(), gomock.Any()).
		Return(h.heimdallNextMockSpan.ValidatorSet.Validators, nil).
		AnyTimes()

	h.borSpanner.
		EXPECT().
		GetCurrentProducers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ uint64, _ bor.ChainHeaderReader) ([]*valset.Validator, error) {
			res := make([]*valset.Validator, len(h.heimdallNextMockSpan.SelectedProducers))
			for i := range h.heimdallNextMockSpan.SelectedProducers {
				res[i] = &h.heimdallNextMockSpan.SelectedProducers[i]
			}

			return res, nil
		}).
		AnyTimes()
}

func (h *Harness) mockHeimdallClient() {
	h.heimdallClient.
		EXPECT().
		FetchSpan(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, spanID uint64) (*heimdall.Span, error) {
			res := h.heimdallNextMockSpan
			h.heimdallNextMockSpan = &heimdall.Span{
				Id:                res.Id + 1,
				StartBlock:        res.EndBlock + 1,
				EndBlock:          res.EndBlock + 6400,
				ValidatorSet:      res.ValidatorSet,
				SelectedProducers: res.SelectedProducers,
			}

			if selectedProducers, ok := h.heimdallProducersOverride[uint64(res.Id)]; ok {
				res.SelectedProducers = selectedProducers
			}

			return res, nil
		}).
		AnyTimes()

	h.heimdallClient.
		EXPECT().
		FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, _ time.Time, _ int) ([]*heimdall.EventRecordWithTime, error) {
			if h.heimdallLastEventID > 0 {
				h.heimdallLastEventHeaderNum += h.borConfig.CalculateSprintLength(h.heimdallLastEventHeaderNum)
			}
			h.heimdallLastEventID++
			stateSyncDelay := h.borConfig.CalculateStateSyncDelay(h.heimdallLastEventHeaderNum)
			newEvent := heimdall.EventRecordWithTime{
				EventRecord: heimdall.EventRecord{
					ID:      h.heimdallLastEventID,
					ChainID: h.chainConfig.ChainID.String(),
				},
				Time: time.Unix(int64(h.sealedHeaders[h.heimdallLastEventHeaderNum].Time-stateSyncDelay-1), 0),
			}

			// 1 per sprint
			return []*heimdall.EventRecordWithTime{&newEvent}, nil
		}).
		AnyTimes()
}

func (h *Harness) runSyncStageForwardWithErrorIs(
	t *testing.T,
	id stages.SyncStage,
	sync *stagedsync.Sync,
	syncStages []*stagedsync.Stage,
	wantErr error,
	txc wrap.TxContainer,
) {
	err := h.runSyncStageForwardWithReturnError(t, id, sync, syncStages, txc)
	require.ErrorIs(t, err, wantErr)
}

func (h *Harness) runSyncStageForwardWithReturnError(
	t *testing.T,
	id stages.SyncStage,
	sync *stagedsync.Sync,
	syncStages []*stagedsync.Stage,
	txc wrap.TxContainer,
) error {
	err := sync.SetCurrentStage(id)
	require.NoError(t, err)

	stage, found := h.findSyncStageByID(id, syncStages)
	require.True(t, found)

	stageState, err := sync.StageState(id, txc.Tx, h.chainDataDB, true, false)
	require.NoError(t, err)

	return stage.Forward(false, stageState, sync, txc, h.logger)
}

func (h *Harness) findSyncStageByID(id stages.SyncStage, syncStages []*stagedsync.Stage) (*stagedsync.Stage, bool) {
	for _, s := range syncStages {
		if s.ID == id {
			return s, true
		}
	}

	return nil, false
}
