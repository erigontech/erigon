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

package polygon

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	ethereum "github.com/erigontech/erigon/v3"
	"github.com/erigontech/erigon/v3/accounts/abi/bind"
	"github.com/erigontech/erigon/v3/cmd/devnet/accounts"
	"github.com/erigontech/erigon/v3/cmd/devnet/blocks"
	"github.com/erigontech/erigon/v3/cmd/devnet/contracts"
	"github.com/erigontech/erigon/v3/cmd/devnet/devnet"
	"github.com/erigontech/erigon/v3/polygon/bor/borcfg"
	"github.com/erigontech/erigon/v3/polygon/bor/valset"
	"github.com/erigontech/erigon/v3/polygon/heimdall"
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

const HeimdallURLDefault = "http://localhost:1317"

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
	borConfig          *borcfg.BorConfig
	listenAddr         string
	validatorSet       *valset.ValidatorSet
	pendingCheckpoint  *heimdall.Checkpoint
	latestCheckpoint   *CheckpointAck
	ackWaiter          *sync.Cond
	currentSpan        *heimdall.Span
	spans              map[heimdall.SpanId]*heimdall.Span
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

func NewHeimdall(
	chainConfig *chain.Config,
	serverURL string,
	checkpointConfig *CheckpointConfig,
	logger log.Logger,
) *Heimdall {
	heimdall := &Heimdall{
		chainConfig:        chainConfig,
		borConfig:          chainConfig.Bor.(*borcfg.BorConfig),
		listenAddr:         serverURL[7:],
		checkpointConfig:   *checkpointConfig,
		spans:              map[heimdall.SpanId]*heimdall.Span{},
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

func (h *Heimdall) FetchSpan(ctx context.Context, spanID uint64) (*heimdall.Span, error) {
	h.Lock()
	defer h.Unlock()

	if span, ok := h.spans[heimdall.SpanId(spanID)]; ok {
		h.currentSpan = span
		return span, nil
	}

	var nextSpan = heimdall.Span{
		Id:           heimdall.SpanId(spanID),
		ValidatorSet: *h.validatorSet,
		ChainID:      h.chainConfig.ChainID.String(),
	}

	if h.currentSpan == nil || spanID == 0 {
		nextSpan.StartBlock = 1 //256
	} else {
		if spanID != uint64(h.currentSpan.Id+1) {
			return nil, errors.New("can't initialize span: non consecutive span")
		}

		nextSpan.StartBlock = h.currentSpan.EndBlock + 1
	}

	nextSpan.EndBlock = nextSpan.StartBlock + (100 * h.borConfig.CalculateSprintLength(nextSpan.StartBlock)) - 1

	// TODO we should use a subset here - see: https://wiki.polygon.technology/docs/pos/bor/

	nextSpan.SelectedProducers = make([]valset.Validator, len(h.validatorSet.Validators))

	for i, v := range h.validatorSet.Validators {
		nextSpan.SelectedProducers[i] = *v
	}

	h.currentSpan = &nextSpan

	h.spans[h.currentSpan.Id] = h.currentSpan

	return h.currentSpan, nil
}

func (h *Heimdall) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Span, error) {
	return nil, errors.New("TODO")
}

func (h *Heimdall) FetchLatestSpan(ctx context.Context) (*heimdall.Span, error) {
	return nil, errors.New("TODO")
}

func (h *Heimdall) currentSprintLength() int {
	if h.currentSpan != nil {
		return int(h.borConfig.CalculateSprintLength(h.currentSpan.StartBlock))
	}

	return int(h.borConfig.CalculateSprintLength(256))
}

func (h *Heimdall) getSpanOverrideHeight() uint64 {
	return 0
	//MainChain: 8664000
}

func (h *Heimdall) FetchCheckpoint(ctx context.Context, number int64) (*heimdall.Checkpoint, error) {
	return nil, errors.New("TODO")
}

func (h *Heimdall) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, errors.New("TODO")
}

func (h *Heimdall) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Checkpoint, error) {
	return nil, errors.New("TODO")
}

func (h *Heimdall) FetchMilestone(ctx context.Context, number int64) (*heimdall.Milestone, error) {
	return nil, errors.New("TODO")
}

func (h *Heimdall) FetchMilestoneCount(ctx context.Context) (int64, error) {
	return 0, errors.New("TODO")
}

func (h *Heimdall) FetchFirstMilestoneNum(ctx context.Context) (int64, error) {
	return 0, errors.New("TODO")
}

func (h *Heimdall) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return errors.New("TODO")
}

func (h *Heimdall) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	return "", errors.New("TODO")
}

func (h *Heimdall) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return errors.New("TODO")
}

func (h *Heimdall) FetchStateSyncEvents(ctx context.Context, fromID uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error) {
	return nil, errors.New("TODO")
}

func (h *Heimdall) FetchStateSyncEvent(ctx context.Context, id uint64) (*heimdall.EventRecordWithTime, error) {
	return nil, errors.New("TODO")
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

	if strings.HasPrefix(node.GetName(), "bor") && node.IsBlockProducer() && node.Account() != nil {
		// TODO configurable voting power
		h.addValidator(node.Account().Address, 1000, 0)
	}
}

func (h *Heimdall) NodeStarted(ctx context.Context, node devnet.Node) {
	if h.validatorSet == nil {
		panic("Heimdall devnet service: unexpected empty validator set! Call addValidator() before starting nodes.")
	}

	if !strings.HasPrefix(node.GetName(), "bor") && node.IsBlockProducer() {
		h.Lock()
		defer h.Unlock()

		if h.syncSenderBinding != nil {
			return
		}

		h.startTime = time.Now().UTC()

		transactOpts, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(node.Account().Address), node.ChainID())

		if err != nil {
			h.Unlock()
			h.unsubscribe()
			h.Lock()
			h.logger.Error("Failed to create transact opts for deploying state sender", "err", err)
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

			go h.startStateSyncSubscription()
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
		})
	} else {
		h.validatorSet.UpdateWithChangeSet([]*valset.Validator{
			{
				ID:               uint64(len(h.validatorSet.Validators) + 1),
				Address:          validatorAddress,
				VotingPower:      votingPower,
				ProposerPriority: proposerPriority,
			},
		})
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

	server := &http.Server{Addr: h.listenAddr, Handler: makeHeimdallRouter(ctx, h)}
	return startHTTPServer(ctx, server, "devnet Heimdall service", h.logger)
}

func makeHeimdallRouter(ctx context.Context, client heimdall.HeimdallClient) *chi.Mux {
	router := chi.NewRouter()

	writeResponse := func(w http.ResponseWriter, result any, err error) {
		if err != nil {
			http.Error(w, http.StatusText(500), 500)
			return
		}

		var resultEnvelope struct {
			Height string `json:"height"`
			Result any    `json:"result"`
		}
		resultEnvelope.Height = "0"
		resultEnvelope.Result = result

		response, err := json.Marshal(resultEnvelope)
		if err != nil {
			http.Error(w, http.StatusText(500), 500)
			return
		}

		_, _ = w.Write(response)
	}

	wrapResult := func(result any) map[string]any {
		return map[string]any{
			"result": result,
		}
	}

	router.Get("/clerk/event-record/list", func(w http.ResponseWriter, r *http.Request) {
		fromIdStr := r.URL.Query().Get("from-id")
		fromId, err := strconv.ParseUint(fromIdStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}

		toTimeStr := r.URL.Query().Get("to-time")
		toTime, err := strconv.ParseInt(toTimeStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}

		result, err := client.FetchStateSyncEvents(ctx, fromId, time.Unix(toTime, 0), 0)
		writeResponse(w, result, err)
	})

	router.Get("/bor/span/{id}", func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}
		result, err := client.FetchSpan(ctx, id)
		writeResponse(w, result, err)
	})

	router.Get("/checkpoints/{number}", func(w http.ResponseWriter, r *http.Request) {
		numberStr := chi.URLParam(r, "number")
		number, err := strconv.ParseInt(numberStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}
		result, err := client.FetchCheckpoint(ctx, number)
		writeResponse(w, result, err)
	})

	router.Get("/checkpoints/latest", func(w http.ResponseWriter, r *http.Request) {
		result, err := client.FetchCheckpoint(ctx, -1)
		writeResponse(w, result, err)
	})

	router.Get("/checkpoints/count", func(w http.ResponseWriter, r *http.Request) {
		result, err := client.FetchCheckpointCount(ctx)
		writeResponse(w, wrapResult(result), err)
	})

	router.Get("/checkpoints/list", func(w http.ResponseWriter, r *http.Request) {
		pageStr := r.URL.Query().Get("page")
		page, err := strconv.ParseUint(pageStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}

		limitStr := r.URL.Query().Get("limit")
		limit, err := strconv.ParseUint(limitStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}

		result, err := client.FetchCheckpoints(ctx, page, limit)
		writeResponse(w, wrapResult(result), err)
	})

	router.Get("/milestone/{number}", func(w http.ResponseWriter, r *http.Request) {
		numberStr := chi.URLParam(r, "number")
		number, err := strconv.ParseInt(numberStr, 10, 64)
		if err != nil {
			http.Error(w, http.StatusText(400), 400)
			return
		}
		result, err := client.FetchMilestone(ctx, number)
		writeResponse(w, result, err)
	})

	router.Get("/milestone/latest", func(w http.ResponseWriter, r *http.Request) {
		result, err := client.FetchMilestone(ctx, -1)
		writeResponse(w, result, err)
	})

	router.Get("/milestone/count", func(w http.ResponseWriter, r *http.Request) {
		result, err := client.FetchMilestoneCount(ctx)
		writeResponse(w, heimdall.MilestoneCount{Count: result}, err)
	})

	router.Get("/milestone/noAck/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		err := client.FetchNoAckMilestone(ctx, id)
		result := err == nil
		writeResponse(w, wrapResult(result), err)
	})

	router.Get("/milestone/lastNoAck", func(w http.ResponseWriter, r *http.Request) {
		result, err := client.FetchLastNoAckMilestone(ctx)
		writeResponse(w, wrapResult(result), err)
	})

	router.Get("/milestone/ID/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		err := client.FetchMilestoneID(ctx, id)
		result := err == nil
		writeResponse(w, wrapResult(result), err)
	})

	return router
}

func startHTTPServer(ctx context.Context, server *http.Server, serverName string, logger log.Logger) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}

	go func() {
		err := server.Serve(listener)
		if (err != nil) && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server.Serve error", "serverName", serverName, "err", err)
		}
	}()

	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()

	return nil
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
