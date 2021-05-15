package download

import (
	"context"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	sentry.UnimplementedSentryServer
}

func (ms *MockSentry) PenalizePeer(context.Context, *sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) PeerMinBlock(context.Context, *sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageByMinBlock(context.Context, *sentry.SendMessageByMinBlockRequest) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageById(context.Context, *sentry.SendMessageByIdRequest) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageToRandomPeers(context.Context, *sentry.SendMessageToRandomPeersRequest) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageToAll(context.Context, *sentry.OutboundMessageData) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SetStatus(context.Context, *sentry.StatusData) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) ReceiveMessages(*emptypb.Empty, sentry.Sentry_ReceiveMessagesServer) error {
	return nil
}
func (ms *MockSentry) ReceiveUploadMessages(*emptypb.Empty, sentry.Sentry_ReceiveUploadMessagesServer) error {
	return nil
}
func (ms *MockSentry) ReceiveTxMessages(*emptypb.Empty, sentry.Sentry_ReceiveTxMessagesServer) error {
	return nil
}

// passing tmpdir because it is renponsibility of the caller to clean it up
func testStagedSync(t *testing.T, tmpdir string) *stagedsync.StagedSync {
	ctx := context.Background()
	memDb := ethdb.NewMemDatabase()
	defer memDb.Close()
	db := memDb.RwKV()
	sm := ethdb.DefaultStorageMode
	engine := ethash.NewFaker()
	hd := headerdownload.NewHeaderDownload(1024 /* anchorLimit */, 1024 /* linkLimit */, engine)
	chainConfig := params.AllEthashProtocolChanges
	sendHeaderRequest := func(context.Context, *headerdownload.HeaderRequest) []byte {
		return nil
	}
	propagateNewBlockHashes := func(context.Context, []headerdownload.Announce) {
	}
	penalize := func(context.Context, []headerdownload.PenaltyItem) {
	}
	batchSize := 1 * datasize.MB
	increment := uint64(0)
	bd := bodydownload.NewBodyDownload(1024 /* outstandingLimit */, engine)
	sendBodyRequest := func(context.Context, *bodydownload.BodyRequest) []byte {
		return nil
	}
	updateHead := func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int) {
	}
	blockPropagator := func(ctx context.Context, block *types.Block, td *big.Int) {
	}
	blockDowloadTimeout := 10
	txCacher := core.NewTxSenderCacher(1)
	txPoolConfig := core.DefaultTxPoolConfig
	txPoolConfig.Journal = ""
	txPoolConfig.StartOnInit = true
	txPool := core.NewTxPool(txPoolConfig, chainConfig, memDb, txCacher)
	txSentryClient := &SentryClientDirect{}
	txSentry := &MockSentry{}
	txSentryClient.SetServer(txSentry)
	txPoolServer, err := eth.NewTxPoolServer(ctx, []sentry.SentryClient{txSentryClient}, txPool)
	if err != nil {
		t.Fatal(err)
	}
	return stages.NewStagedSync(ctx, sm,
		stagedsync.StageHeadersCfg(
			db,
			hd,
			*chainConfig,
			sendHeaderRequest,
			propagateNewBlockHashes,
			penalize,
			batchSize,
			increment,
		),
		stagedsync.StageBodiesCfg(
			db,
			bd,
			sendBodyRequest,
			penalize,
			updateHead,
			blockPropagator,
			blockDowloadTimeout,
			*chainConfig,
			batchSize,
		),
		stagedsync.StageSendersCfg(db, chainConfig),
		stagedsync.StageExecuteBlocksCfg(
			db,
			sm.Receipts,
			sm.CallTraces,
			batchSize,
			nil,
			nil,
			nil,
			nil,
			chainConfig,
			engine,
			&vm.Config{NoReceipts: !sm.Receipts},
			tmpdir,
		),
		stagedsync.StageHashStateCfg(db, tmpdir),
		stagedsync.StageTrieCfg(db, true, true, tmpdir),
		stagedsync.StageHistoryCfg(db, tmpdir),
		stagedsync.StageLogIndexCfg(db, tmpdir),
		stagedsync.StageCallTracesCfg(db, 0, batchSize, tmpdir, chainConfig, engine),
		stagedsync.StageTxLookupCfg(db, tmpdir),
		stagedsync.StageTxPoolCfg(db, txPool, func() {
			txPoolServer.Start()
			txPoolServer.TxFetcher.Start()
		}),
		stagedsync.StageFinishCfg(db, tmpdir),
	)
}

func TestEmptyStageSync(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "stagesync-test")
	if err != nil {
		log.Fatal(err)
	}

	defer os.RemoveAll(tmpdir) // clean up
	testStagedSync(t, tmpdir)
}
