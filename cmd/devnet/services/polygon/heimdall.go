package polygon

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdallgrpc"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/log/v3"
)

type BridgeEvent string

var BridgeEvents = struct {
	StakingEvent  BridgeEvent
	TopupEvent    BridgeEvent
	ClerkEvent    BridgeEvent
	SlashingEvent BridgeEvent
}{
	StakingEvent:  "staking",
	TopupEvent:    "topup",
	ClerkEvent:    "clerk",
	SlashingEvent: "slashing",
}

type syncRecordKey struct {
	hash  libcommon.Hash
	index uint64
}

const (
	DefaultRootChainTxConfirmations  uint64        = 6
	DefaultChildChainTxConfirmations uint64        = 10
	DefaultAvgCheckpointLength       uint64        = 256
	DefaultMaxCheckpointLength       uint64        = 1024
	DefaultChildBlockInterval        uint64        = 10000
	DefaultCheckpointBufferTime      time.Duration = 1000 * time.Second
)

type CheckpointConfig struct {
	RootChainTxConfirmations  uint64
	ChildChainTxConfirmations uint64
	ChildBlockInterval        uint64
	AvgCheckpointLength       uint64
	MaxCheckpointLength       uint64
	CheckpointBufferTime      time.Duration
	CheckpointAccount         *accounts.Account
}

type Heimdall struct {
	sync.Mutex
	chainConfig        *chain.Config
	validatorSet       *valset.ValidatorSet
	pendingCheckpoint  *checkpoint.Checkpoint
	latestCheckpoint   *CheckpointAck
	ackWaiter          *sync.Cond
	currentSpan        *span.HeimdallSpan
	spans              map[uint64]*span.HeimdallSpan
	logger             log.Logger
	cancelFunc         context.CancelFunc
	syncSenderAddress  libcommon.Address
	syncSenderBinding  *contracts.TestStateSender
	rootChainAddress   libcommon.Address
	rootChainBinding   *contracts.TestRootChain
	syncSubscription   ethereum.Subscription
	rootHeaderBlockSub ethereum.Subscription
	childHeaderSub     ethereum.Subscription
	pendingSyncRecords map[syncRecordKey]*EventRecordWithBlock
	checkpointConfig   CheckpointConfig
	startTime          time.Time
}

func NewHeimdall(chainConfig *chain.Config, checkpointConfig *CheckpointConfig, logger log.Logger) *Heimdall {
	heimdall := &Heimdall{
		chainConfig:        chainConfig,
		checkpointConfig:   *checkpointConfig,
		spans:              map[uint64]*span.HeimdallSpan{},
		pendingSyncRecords: map[syncRecordKey]*EventRecordWithBlock{},
		logger:             logger}

	heimdall.ackWaiter = sync.NewCond(heimdall)

	if heimdall.checkpointConfig.RootChainTxConfirmations == 0 {
		heimdall.checkpointConfig.RootChainTxConfirmations = DefaultRootChainTxConfirmations
	}

	if heimdall.checkpointConfig.ChildChainTxConfirmations == 0 {
		heimdall.checkpointConfig.ChildChainTxConfirmations = DefaultChildChainTxConfirmations
	}

	if heimdall.checkpointConfig.ChildBlockInterval == 0 {
		heimdall.checkpointConfig.ChildBlockInterval = DefaultChildBlockInterval
	}

	if heimdall.checkpointConfig.AvgCheckpointLength == 0 {
		heimdall.checkpointConfig.AvgCheckpointLength = DefaultAvgCheckpointLength
	}

	if heimdall.checkpointConfig.MaxCheckpointLength == 0 {
		heimdall.checkpointConfig.MaxCheckpointLength = DefaultMaxCheckpointLength
	}

	if heimdall.checkpointConfig.CheckpointBufferTime == 0 {
		heimdall.checkpointConfig.CheckpointBufferTime = DefaultCheckpointBufferTime
	}

	if heimdall.checkpointConfig.CheckpointAccount == nil {
		heimdall.checkpointConfig.CheckpointAccount = accounts.NewAccount("checkpoint-owner")
	}

	return heimdall
}

func (h *Heimdall) Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error) {
	h.Lock()
	defer h.Unlock()

	if span, ok := h.spans[spanID]; ok {
		h.currentSpan = span
		return span, nil
	}

	var nextSpan = span.Span{
		ID: spanID,
	}

	if h.currentSpan == nil || spanID == 0 {
		nextSpan.StartBlock = 1 //256
	} else {
		if spanID != h.currentSpan.ID+1 {
			return nil, fmt.Errorf("Can't initialize span: non consecutive span")
		}

		nextSpan.StartBlock = h.currentSpan.EndBlock + 1
	}

	nextSpan.EndBlock = nextSpan.StartBlock + (100 * h.chainConfig.Bor.CalculateSprint(nextSpan.StartBlock)) - 1

	// TODO we should use a subset here - see: https://wiki.polygon.technology/docs/pos/bor/

	selectedProducers := make([]valset.Validator, len(h.validatorSet.Validators))

	for i, v := range h.validatorSet.Validators {
		selectedProducers[i] = *v
	}

	h.currentSpan = &span.HeimdallSpan{
		Span:              nextSpan,
		ValidatorSet:      *h.validatorSet,
		SelectedProducers: selectedProducers,
		ChainID:           h.chainConfig.ChainID.String(),
	}

	h.spans[h.currentSpan.ID] = h.currentSpan

	return h.currentSpan, nil
}

func (h *Heimdall) currentSprintLength() int {
	if h.currentSpan != nil {
		return int(h.chainConfig.Bor.CalculateSprint(h.currentSpan.StartBlock))
	}

	return int(h.chainConfig.Bor.CalculateSprint(256))
}

func (h *Heimdall) getSpanOverrideHeight() uint64 {
	return 0
	//MainChain: 8664000
	//MumbaiChain: 10205000
}

func (h *Heimdall) FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
	return nil, fmt.Errorf("TODO")
}

func (h *Heimdall) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("TODO")
}

func (h *Heimdall) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	return nil, fmt.Errorf("TODO")
}

func (h *Heimdall) FetchMilestoneCount(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("TODO")
}

func (h *Heimdall) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return fmt.Errorf("TODO")
}

func (h *Heimdall) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	return "", fmt.Errorf("TODO")
}

func (h *Heimdall) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return fmt.Errorf("TODO")
}

func (h *Heimdall) Close() {
	h.unsubscribe()
}

func (h *Heimdall) unsubscribe() {
	h.Lock()
	defer h.Unlock()

	if h.syncSubscription != nil {
		syncSubscription := h.syncSubscription
		h.syncSubscription = nil
		syncSubscription.Unsubscribe()
	}

	if h.rootHeaderBlockSub != nil {
		rootHeaderBlockSub := h.rootHeaderBlockSub
		h.rootHeaderBlockSub = nil
		rootHeaderBlockSub.Unsubscribe()
	}

	if h.childHeaderSub != nil {
		childHeaderSub := h.childHeaderSub
		h.childHeaderSub = nil
		childHeaderSub.Unsubscribe()
	}
}

func (h *Heimdall) StateSenderAddress() libcommon.Address {
	return h.syncSenderAddress
}

func (f *Heimdall) StateSenderContract() *contracts.TestStateSender {
	return f.syncSenderBinding
}

func (h *Heimdall) RootChainAddress() libcommon.Address {
	return h.rootChainAddress
}

func (h *Heimdall) NodeCreated(ctx context.Context, node devnet.Node) {
	h.Lock()
	defer h.Unlock()

	if strings.HasPrefix(node.Name(), "bor") && node.IsBlockProducer() && node.Account() != nil {
		// TODO configurable voting power
		h.addValidator(node.Account().Address, 1000, 0)
	}
}

func (h *Heimdall) NodeStarted(ctx context.Context, node devnet.Node) {
	if !strings.HasPrefix(node.Name(), "bor") && node.IsBlockProducer() {
		h.Lock()
		defer h.Unlock()

		if h.syncSenderBinding != nil {
			return
		}

		h.startTime = time.Now().UTC()

		transactOpts, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(node.Account().Address), node.ChainID())

		if err != nil {
			h.unsubscribe()
			h.logger.Error("Failed to deploy state sender", "err", err)
			return
		}

		deployCtx := devnet.WithCurrentNode(ctx, node)
		waiter, cancel := blocks.BlockWaiter(deployCtx, contracts.DeploymentChecker)

		address, syncTx, syncContract, err := contracts.DeployWithOps(deployCtx, transactOpts, contracts.DeployTestStateSender)

		if err != nil {
			h.logger.Error("Failed to deploy state sender", "err", err)
			cancel()
			return
		}

		h.syncSenderAddress = address
		h.syncSenderBinding = syncContract

		address, rootChainTx, rootChainContract, err := contracts.DeployWithOps(deployCtx, transactOpts, contracts.DeployTestRootChain)

		if err != nil {
			h.syncSenderBinding = nil
			h.logger.Error("Failed to deploy root chain", "err", err)
			cancel()
			return
		}

		h.rootChainAddress = address
		h.rootChainBinding = rootChainContract

		go func() {
			defer cancel()
			blocks, err := waiter.AwaitMany(syncTx.Hash(), rootChainTx.Hash())

			if err != nil {
				h.syncSenderBinding = nil
				h.logger.Error("Failed to deploy root contracts", "err", err)
				return
			}

			h.logger.Info("RootChain deployed", "chain", h.chainConfig.ChainName, "block", blocks[syncTx.Hash()].Number, "addr", h.rootChainAddress)
			h.logger.Info("StateSender deployed", "chain", h.chainConfig.ChainName, "block", blocks[syncTx.Hash()].Number, "addr", h.syncSenderAddress)

			go h.startStateSyncSubacription()
			go h.startChildHeaderSubscription(deployCtx)
			go h.startRootHeaderBlockSubscription()
		}()
	}
}

func (h *Heimdall) addValidator(validatorAddress libcommon.Address, votingPower int64, proposerPriority int64) {

	if h.validatorSet == nil {
		h.validatorSet = valset.NewValidatorSet([]*valset.Validator{
			{
				ID:               1,
				Address:          validatorAddress,
				VotingPower:      votingPower,
				ProposerPriority: proposerPriority,
			},
		}, h.logger)
	} else {
		h.validatorSet.UpdateWithChangeSet([]*valset.Validator{
			{
				ID:               uint64(len(h.validatorSet.Validators) + 1),
				Address:          validatorAddress,
				VotingPower:      votingPower,
				ProposerPriority: proposerPriority,
			},
		}, h.logger)
	}
}

func (h *Heimdall) Start(ctx context.Context) error {
	h.Lock()
	if h.cancelFunc != nil {
		h.Unlock()
		return nil
	}
	ctx, h.cancelFunc = context.WithCancel(ctx)
	h.Unlock()

	// if this is a restart
	h.unsubscribe()

	return heimdallgrpc.StartHeimdallServer(ctx, h, HeimdallGRpc(ctx), h.logger)
}

func HeimdallGRpc(ctx context.Context) string {
	addr := "localhost:8540"

	if cli := devnet.CliContext(ctx); cli != nil {
		if grpcAddr := cli.String("bor.heimdallgRPC"); len(grpcAddr) > 0 {
			addr = grpcAddr
		}
	}

	return addr
}

func (h *Heimdall) Stop() {
	var cancel context.CancelFunc

	h.Lock()
	if h.cancelFunc != nil {
		cancel = h.cancelFunc
		h.cancelFunc = nil
	}

	h.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (h *Heimdall) AwaitCheckpoint(ctx context.Context, blockNumber *big.Int) error {
	h.Lock()
	defer h.Unlock()

	if ctx.Done() != nil {
		go func() {
			defer h.ackWaiter.Broadcast()
			<-ctx.Done()
		}()
	}

	for h.latestCheckpoint == nil || h.latestCheckpoint.EndBlock < blockNumber.Uint64() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		h.ackWaiter.Wait()
	}

	return nil
}

func (h *Heimdall) isOldTx(txHash libcommon.Hash, logIndex uint64, eventType BridgeEvent, event interface{}) (bool, error) {

	// define the endpoint based on the type of event
	var status bool

	switch eventType {
	case BridgeEvents.StakingEvent:
	case BridgeEvents.TopupEvent:
	case BridgeEvents.ClerkEvent:
		_, status = h.pendingSyncRecords[syncRecordKey{txHash, logIndex}]
	case BridgeEvents.SlashingEvent:
	}

	return status, nil
}
