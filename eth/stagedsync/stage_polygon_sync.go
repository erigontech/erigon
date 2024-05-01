package stagedsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
	"github.com/ledgerwatch/erigon/polygon/sync"
)

func NewPolygonSyncStageCfg(
	logger log.Logger,
	chainConfig *chain.Config,
	db kv.RwDB,
	heimdallClient heimdall.HeimdallClient,
	sentry direct.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
) PolygonSyncStageCfg {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	storage := newPolygonSyncStageStorage(logger)
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	headersVerifier := sync.VerifyAccumulatedHeaders
	blocksVerifier := sync.VerifyBlocks
	heimdallService := heimdall.NewHeimdall(heimdallClient, logger, heimdall.WithStore(storage))
	blockDownloader := sync.NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		blocksVerifier,
		storage,
	)
	spansCache := sync.NewSpansCache()
	events := sync.NewTipEvents(logger, p2pService, heimdallService)
	sync := sync.NewSync(
		storage,
		&noopExecutionClient{},
		headersVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		sync.NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache),
		spansCache,
		heimdallService.FetchLatestSpan,
		events.Events(),
		logger,
	)
	syncService := newPolygonSyncStageService(sync)
	return PolygonSyncStageCfg{
		db:      db,
		storage: storage,
		service: syncService,
	}
}

type PolygonSyncStageCfg struct {
	db      kv.RwDB
	storage *polygonSyncStageStorage
	service *polygonSyncStageService
}

func SpawnPolygonSyncStage(ctx context.Context, tx kv.RwTx, cfg PolygonSyncStageCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	cfg.storage.use(tx)

	if !useExternalTx {
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func UnwindPolygonSyncStage() error {
	return nil
}

func PrunePolygonSyncStage() error {
	return nil
}

func newPolygonSyncStageService(sync *sync.Sync) *polygonSyncStageService {
	return &polygonSyncStageService{
		sync: sync,
	}
}

type polygonSyncStageService struct {
	sync *sync.Sync
}

func (s polygonSyncStageService) Run(ctx context.Context) error {
	return nil
}

func newPolygonSyncStageStorage(logger log.Logger) *polygonSyncStageStorage {
	return &polygonSyncStageStorage{
		logger: logger,
	}
}

type polygonSyncStageStorage struct {
	logger log.Logger

	heimdall.Store
	tx kv.RwTx
}

func (s *polygonSyncStageStorage) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	if s.tx == nil {
		return errors.New("missing tx")
	}

	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, s.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, s.logger)

		parentTd := common.Big0
		if height > 0 {
			// Parent's total difficulty
			parentTd, err := rawdb.ReadTd(s.tx, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return fmt.Errorf(
					"parent's total difficulty not found with hash %x and height %d: %v",
					header.ParentHash,
					height-1,
					err,
				)
			}
		}

		td := parentTd.Add(parentTd, header.Difficulty)
		if err := rawdb.WriteHeader(s.tx, header); err != nil {
			return fmt.Errorf("ethereumExecutionModule.InsertHeaders: writeHeader: %s", err)
		}

		if err := rawdb.WriteTd(s.tx, header.Hash(), height, td); err != nil {
			return fmt.Errorf("ethereumExecutionModule.InsertHeaders: writeTd: %s", err)
		}

		if _, err := rawdb.WriteRawBodyIfNotExists(s.tx, header.Hash(), height, body.RawBody()); err != nil {
			return fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
	}

	return nil
}

func (s *polygonSyncStageStorage) Flush(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) Run(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) use(tx kv.RwTx) {
	s.tx = tx
	// TODO pass in reader
	s.Store = heimdall.NewTxStore(nil, tx)
}

type noopExecutionClient struct{}

func (ec noopExecutionClient) InsertBlocks(context.Context, []*types.Block) error {
	panic("should not be used")
}

func (ec noopExecutionClient) UpdateForkChoice(context.Context, *types.Header, *types.Header) error {
	return nil
}

func (ec noopExecutionClient) CurrentHeader(context.Context) (*types.Header, error) {
	// TODO need to change sync.Run to use some other func for getting current persisted header instead of canonical/executed?
	panic("should not be used")
}
