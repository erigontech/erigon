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

	"github.com/golang/mock/gomock"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	heimdallmock "github.com/ledgerwatch/erigon/consensus/bor/heimdall/mock"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	bormock "github.com/ledgerwatch/erigon/consensus/bor/mock"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	consensusmock "github.com/ledgerwatch/erigon/consensus/mock"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func InitHarness(ctx context.Context, t *testing.T, cfg HarnessCfg) Harness {
	logger := testlog.Logger(t, cfg.LogLvl)
	genesisInit := createGenesisInitData(t, cfg.ChainConfig)
	m := mock.MockWithGenesis(t, genesisInit.genesis, genesisInit.genesisAllocPrivateKey, false)
	chainDataDB := m.DB
	blockReader := m.BlockReader
	borConsensusDB := memdb.NewTestDB(t)
	ctrl := gomock.NewController(t)
	heimdallClient := heimdallmock.NewMockIHeimdallClient(ctrl)
	bhCfg := stagedsync.StageBorHeimdallCfg(
		chainDataDB,
		borConsensusDB,
		stagedsync.NewProposingState(&ethconfig.Defaults.Miner),
		*cfg.ChainConfig,
		heimdallClient,
		blockReader,
		nil, // headerDownloader
		nil, // penalize
		nil, // not used
		nil, // not used
		nil,
	)
	stateSyncStages := stagedsync.DefaultStages(
		ctx,
		stagedsync.SnapshotsCfg{},
		stagedsync.HeadersCfg{},
		bhCfg,
		stagedsync.BlockHashesCfg{},
		stagedsync.BodiesCfg{},
		stagedsync.SendersCfg{},
		stagedsync.ExecuteBlockCfg{},
		stagedsync.HashStateCfg{},
		stagedsync.TrieCfg{},
		stagedsync.HistoryCfg{},
		stagedsync.LogIndexCfg{},
		stagedsync.CallTracesCfg{},
		stagedsync.TxLookupCfg{},
		stagedsync.FinishCfg{},
		true,
	)
	stateSync := stagedsync.New(ethconfig.Defaults.Sync, stateSyncStages, stagedsync.DefaultUnwindOrder, stagedsync.DefaultPruneOrder, logger)
	validatorKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	validatorAddress := crypto.PubkeyToAddress(validatorKey.PublicKey)
	h := Harness{
		logger:                    logger,
		chainDataDB:               chainDataDB,
		borConsensusDB:            borConsensusDB,
		chainConfig:               cfg.ChainConfig,
		blockReader:               blockReader,
		stateSyncStages:           stateSyncStages,
		stateSync:                 stateSync,
		bhCfg:                     bhCfg,
		heimdallClient:            heimdallClient,
		heimdallProducersOverride: cfg.GetOrCreateDefaultHeimdallProducersOverride(),
		sealedHeaders:             make(map[uint64]*types.Header),
		borSpanner:                bormock.NewMockSpanner(ctrl),
		validatorAddress:          validatorAddress,
		validatorKey:              validatorKey,
		genesisInitData:           genesisInit,
	}

	if cfg.ChainConfig.Bor != nil {
		h.setHeimdallNextMockSpan(logger)
		h.mockBorSpanner()
		h.mockHeimdallClient()
	}

	h.generateChain(ctx, t, ctrl, cfg)

	return h
}

type genesisInitData struct {
	genesis                 *types.Genesis
	genesisAllocPrivateKey  *ecdsa.PrivateKey
	genesisAllocPrivateKeys map[libcommon.Address]*ecdsa.PrivateKey
	fundedAddresses         []libcommon.Address
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
	chainDataDB                kv.RwDB
	borConsensusDB             kv.RwDB
	chainConfig                *chain.Config
	blockReader                services.BlockReader
	stateSyncStages            []*stagedsync.Stage
	stateSync                  *stagedsync.Sync
	bhCfg                      stagedsync.BorHeimdallCfg
	heimdallClient             *heimdallmock.MockIHeimdallClient
	heimdallNextMockSpan       *span.HeimdallSpan
	heimdallLastEventID        uint64
	heimdallLastEventHeaderNum uint64
	heimdallProducersOverride  map[uint64][]valset.Validator // spanID -> selected producers override
	sealedHeaders              map[uint64]*types.Header
	borSpanner                 *bormock.MockSpanner
	validatorAddress           libcommon.Address
	validatorKey               *ecdsa.PrivateKey
	genesisInitData            *genesisInitData
}

func (h *Harness) Logger() log.Logger {
	return h.logger
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

func (h *Harness) RunStageForward(t *testing.T, id stages.SyncStage) {
	h.RunStageForwardWithErrorIs(t, id, nil)
}

func (h *Harness) RunStageForwardWithErrorIs(t *testing.T, id stages.SyncStage, wantErr error) {
	err := h.RunStageForwardWithReturnError(t, id)
	require.ErrorIs(t, err, wantErr)
}

func (h *Harness) RunStageForwardWithReturnError(t *testing.T, id stages.SyncStage) error {
	err := h.stateSync.SetCurrentStage(id)
	require.NoError(t, err)

	stage, found := h.findStateSyncStageByID(id)
	require.True(t, found)

	stageState, err := h.stateSync.StageState(id, nil, h.chainDataDB)
	require.NoError(t, err)

	return stage.Forward(true, false, stageState, h.stateSync, nil, h.logger)
}

func (h *Harness) ReadSpansFromDB(ctx context.Context) (spans []*span.HeimdallSpan, err error) {
	err = h.chainDataDB.View(ctx, func(tx kv.Tx) error {
		spanIter, err := tx.Range(kv.BorSpans, nil, nil)
		if err != nil {
			return err
		}

		for spanIter.HasNext() {
			keyBytes, spanBytes, err := spanIter.Next()
			if err != nil {
				return err
			}

			spanKey := binary.BigEndian.Uint64(keyBytes)
			var heimdallSpan span.HeimdallSpan
			if err = json.Unmarshal(spanBytes, &heimdallSpan); err != nil {
				return err
			}

			if spanKey != heimdallSpan.ID {
				return fmt.Errorf("span key and id mismatch %d!=%d", spanKey, heimdallSpan.ID)
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
		eventsIter, err := tx.Range(kv.BorEvents, nil, nil)
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

func (h *Harness) ReadFirstStateSyncEventNumPerBlockFromDB(ctx context.Context) (nums map[uint64]uint64, err error) {
	nums = map[uint64]uint64{}
	err = h.chainDataDB.View(ctx, func(tx kv.Tx) error {
		eventNumsIter, err := tx.Range(kv.BorEventNums, nil, nil)
		if err != nil {
			return err
		}

		for eventNumsIter.HasNext() {
			blockNumBytes, firstEventNumBytes, err := eventNumsIter.Next()
			if err != nil {
				return err
			}

			blockNum := binary.BigEndian.Uint64(blockNumBytes)
			firstEventNum := binary.BigEndian.Uint64(firstEventNumBytes)
			nums[blockNum] = firstEventNum
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
		genesisAllocPrivateKeys: map[libcommon.Address]*ecdsa.PrivateKey{
			accountAddress: accountPrivateKey,
		},
		fundedAddresses: []libcommon.Address{
			accountAddress,
		},
	}
}

func (h *Harness) generateChain(ctx context.Context, t *testing.T, ctrl *gomock.Controller, cfg HarnessCfg) {
	consensusEngine := h.consensusEngine(t, cfg)
	var parentBlock *types.Block
	err := h.chainDataDB.View(ctx, func(tx kv.Tx) (err error) {
		parentBlock, err = h.blockReader.BlockByNumber(ctx, tx, 0)
		return err
	})
	require.NoError(t, err)
	h.sealedHeaders[parentBlock.Number().Uint64()] = parentBlock.Header()
	mockChainHR := h.mockChainHeaderReader(ctrl)

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

			h.logger.Info("Adding 1 mock tx to block", "blockNum", gen.GetHeader().Number)
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
	sealRes, sealStop := make(chan *types.Block, 1), make(chan struct{}, 1)
	if err := eng.Seal(chr, block, sealRes, sealStop); err != nil {
		t.Fatal(err)
	}

	sealedParentBlock := <-sealRes
	h.sealedHeaders[sealedParentBlock.Number().Uint64()] = sealedParentBlock.Header()
}

func (h *Harness) consensusEngine(t *testing.T, cfg HarnessCfg) consensus.Engine {
	if h.chainConfig.Bor != nil {
		genesisContracts := contract.NewGenesisContractsClient(
			h.chainConfig,
			h.chainConfig.Bor.ValidatorContract,
			h.chainConfig.Bor.StateReceiverContract,
			h.logger,
		)

		borConsensusEng := bor.New(
			h.chainConfig,
			h.borConsensusDB,
			nil,
			h.borSpanner,
			h.heimdallClient,
			genesisContracts,
			h.logger,
		)

		borConsensusEng.Authorize(h.validatorAddress, func(_ libcommon.Address, _ string, msg []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(msg), h.validatorKey)
		})

		return borConsensusEng
	}

	t.Fatalf("unimplmented consensus engine init for cfg %v", cfg.ChainConfig)
	return nil
}

func (h *Harness) SaveHeader(ctx context.Context, t *testing.T, header *types.Header) {
	h.saveHeaders(ctx, t, []*types.Header{header})
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
	mockChainHR := consensusmock.NewMockChainHeaderReader(ctrl)
	mockChainHR.
		EXPECT().
		GetHeader(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ libcommon.Hash, number uint64) *types.Header {
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

func (h *Harness) setHeimdallNextMockSpan(logger log.Logger) {
	validators := []*valset.Validator{
		{
			ID:               1,
			Address:          h.validatorAddress,
			VotingPower:      1000,
			ProposerPriority: 1,
		},
	}

	validatorSet := valset.NewValidatorSet(validators, logger)
	selectedProducers := make([]valset.Validator, len(validators))
	for i := range validators {
		selectedProducers[i] = *validators[i]
	}

	h.heimdallNextMockSpan = &span.HeimdallSpan{
		Span: span.Span{
			ID:         0,
			StartBlock: 0,
			EndBlock:   255,
		},
		ValidatorSet:      *validatorSet,
		SelectedProducers: selectedProducers,
	}
}

func (h *Harness) mockBorSpanner() {
	h.borSpanner.
		EXPECT().
		GetCurrentValidators(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(h.heimdallNextMockSpan.ValidatorSet.Validators, nil).
		AnyTimes()

	h.borSpanner.
		EXPECT().
		GetCurrentProducers(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ uint64, _ libcommon.Address, _ consensus.ChainHeaderReader) ([]*valset.Validator, error) {
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
		Span(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error) {
			res := h.heimdallNextMockSpan
			h.heimdallNextMockSpan = &span.HeimdallSpan{
				Span: span.Span{
					ID:         res.ID + 1,
					StartBlock: res.EndBlock + 1,
					EndBlock:   res.EndBlock + 6400,
				},
				ValidatorSet:      res.ValidatorSet,
				SelectedProducers: res.SelectedProducers,
			}

			if selectedProducers, ok := h.heimdallProducersOverride[res.ID]; ok {
				res.SelectedProducers = selectedProducers
			}

			return res, nil
		}).
		AnyTimes()

	h.heimdallClient.
		EXPECT().
		StateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint64, _ int64) ([]*clerk.EventRecordWithTime, error) {
			h.heimdallLastEventID++
			h.heimdallLastEventHeaderNum += h.chainConfig.Bor.CalculateSprint(h.heimdallLastEventHeaderNum)
			stateSyncDelay := h.chainConfig.Bor.CalculateStateSyncDelay(h.heimdallLastEventHeaderNum)
			newEvent := clerk.EventRecordWithTime{
				EventRecord: clerk.EventRecord{
					ID:      h.heimdallLastEventID,
					ChainID: h.chainConfig.ChainID.String(),
				},
				Time: time.Unix(int64(h.sealedHeaders[h.heimdallLastEventHeaderNum].Time-stateSyncDelay-1), 0),
			}

			// 1 per sprint
			return []*clerk.EventRecordWithTime{&newEvent}, nil
		}).
		AnyTimes()
}

func (h *Harness) findStateSyncStageByID(id stages.SyncStage) (*stagedsync.Stage, bool) {
	for _, s := range h.stateSyncStages {
		if s.ID == id {
			return s, true
		}
	}

	return nil, false
}
