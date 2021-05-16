package download

import (
	"context"
	"fmt"
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
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	sentry.UnimplementedSentryServer
	ctx         context.Context
	memDb       ethdb.Database
	tmpdir      string
	chainConfig *params.ChainConfig
	hd          *headerdownload.HeaderDownload
	sync        *stagedsync.StagedSync
}

func (ms *MockSentry) Close() {
	ms.memDb.Close()
	os.RemoveAll(ms.tmpdir)
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

func mock(t *testing.T) *MockSentry {
	mockSentry := &MockSentry{}
	mockSentry.ctx = context.Background()
	mockSentry.memDb = ethdb.NewMemDatabase()
	var err error
	mockSentry.tmpdir, err = ioutil.TempDir("", "stagesync-test")
	if err != nil {
		log.Fatal(err)
	}
	db := mockSentry.memDb.RwKV()
	sm := ethdb.DefaultStorageMode
	engine := ethash.NewFaker()
	hd := headerdownload.NewHeaderDownload(1024 /* anchorLimit */, 1024 /* linkLimit */, engine)
	mockSentry.chainConfig = params.AllEthashProtocolChanges
	sendHeaderRequest := func(_ context.Context, r *headerdownload.HeaderRequest) []byte {
		fmt.Printf("sendHeaderRequest %+v\n", r)
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
	txPool := core.NewTxPool(txPoolConfig, mockSentry.chainConfig, mockSentry.memDb, txCacher)
	txSentryClient := &SentryClientDirect{}
	txSentryClient.SetServer(mockSentry)
	txPoolServer, err := eth.NewTxPoolServer(mockSentry.ctx, []sentry.SentryClient{txSentryClient}, txPool)
	if err != nil {
		t.Fatal(err)
	}
	mockSentry.sync = stages.NewStagedSync(mockSentry.ctx, sm,
		stagedsync.StageHeadersCfg(
			db,
			hd,
			*mockSentry.chainConfig,
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
			*mockSentry.chainConfig,
			batchSize,
		),
		stagedsync.StageSendersCfg(db, mockSentry.chainConfig),
		stagedsync.StageExecuteBlocksCfg(
			db,
			sm.Receipts,
			sm.CallTraces,
			batchSize,
			nil,
			nil,
			nil,
			nil,
			mockSentry.chainConfig,
			engine,
			&vm.Config{NoReceipts: !sm.Receipts},
			mockSentry.tmpdir,
		),
		stagedsync.StageHashStateCfg(db, mockSentry.tmpdir),
		stagedsync.StageTrieCfg(db, true, true, mockSentry.tmpdir),
		stagedsync.StageHistoryCfg(db, mockSentry.tmpdir),
		stagedsync.StageLogIndexCfg(db, mockSentry.tmpdir),
		stagedsync.StageCallTracesCfg(db, 0, batchSize, mockSentry.tmpdir, mockSentry.chainConfig, engine),
		stagedsync.StageTxLookupCfg(db, mockSentry.tmpdir),
		stagedsync.StageTxPoolCfg(db, txPool, func() {
			txPoolServer.Start()
			txPoolServer.TxFetcher.Start()
		}),
		stagedsync.StageFinishCfg(db, mockSentry.tmpdir),
	)
	return mockSentry
}

func TestEmptyStageSync(t *testing.T) {
	m := mock(t)
	defer m.Close()
}

func TestHeaderStep(t *testing.T) {
	m := mock(t)
	defer m.Close()
	highestSeenHeader := uint64(100)
	notifier := &remotedbserver.Events{}
	initialCycle := true
	if err := stages.StageLoopStep(m.ctx, m.memDb, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}
}
