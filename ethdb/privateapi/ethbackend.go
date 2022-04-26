package privateapi

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type assemblePayloadPOSFunc func(param *core.BlockProposerParametersPOS) (*types.Block, error)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
// 3.1.0 - add Subscribe to logs
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 1, Patch: 0}

const MaxPendingPayloads = 128

var UnknownPayload = rpc.CustomError{Code: -32001, Message: "Unknown payload"}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx         context.Context
	eth         EthBackend
	events      *Events
	db          kv.RoDB
	blockReader interfaces.BlockAndTxnReader
	config      *params.ChainConfig
	// Block proposing for proof-of-stake
	payloadId       uint64
	pendingPayloads map[uint64]*pendingPayload
	// Send Beacon Chain requests to staged sync
	requestList *engineapi.RequestList
	// Replies to newPayload & forkchoice requests
	statusCh           <-chan PayloadStatus
	assemblePayloadPOS assemblePayloadPOSFunc
	proposing          bool
	syncCond           *sync.Cond // Engine API is asynchronous, we want to avoid CL to call different APIs at the same time
	shutdown           bool
	logsFilter         *LogsFilterAggregator
}

type EthBackend interface {
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
	Peers(ctx context.Context) (*remote.PeersReply, error)
}

// This is the status of a newly execute block.
// Hash: Block hash
// Status: block's status
type PayloadStatus struct {
	Status          remote.EngineStatus
	LatestValidHash common.Hash
	ValidationError error
	CriticalError   error
}

type pendingPayload struct {
	block *types.Block
	built bool
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *Events, blockReader interfaces.BlockAndTxnReader,
	config *params.ChainConfig, requestList *engineapi.RequestList, statusCh <-chan PayloadStatus,
	assemblePayloadPOS assemblePayloadPOSFunc, proposing bool,
) *EthBackendServer {
	s := &EthBackendServer{ctx: ctx, eth: eth, events: events, db: db, blockReader: blockReader, config: config,
		requestList: requestList, statusCh: statusCh, pendingPayloads: make(map[uint64]*pendingPayload),
		assemblePayloadPOS: assemblePayloadPOS, proposing: proposing, syncCond: sync.NewCond(&sync.Mutex{}),
		logsFilter: NewLogsFilterAggregator(events),
	}

	ch, clean := s.events.AddLogsSubscription()
	go func() {
		var err error
		defer clean()
		log.Info("new subscription to logs established")
		defer func() {
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Warn("subscription to logs closed", "reason", err)
				}
			} else {
				log.Warn("subscription to logs closed")
			}
		}()
		for {
			select {
			case <-s.ctx.Done():
				err = s.ctx.Err()
				return
			case logs := <-ch:
				s.logsFilter.distributeLogs(logs)
			}
		}
	}()
	return s
}

func (s *EthBackendServer) Version(context.Context, *emptypb.Empty) (*types2.VersionReply, error) {
	return EthBackendAPIVersion, nil
}

func (s *EthBackendServer) Etherbase(_ context.Context, _ *remote.EtherbaseRequest) (*remote.EtherbaseReply, error) {
	out := &remote.EtherbaseReply{Address: gointerfaces.ConvertAddressToH160(common.Address{})}

	base, err := s.eth.Etherbase()
	if err != nil {
		return out, err
	}

	out.Address = gointerfaces.ConvertAddressToH160(base)
	return out, nil
}

func (s *EthBackendServer) NetVersion(_ context.Context, _ *remote.NetVersionRequest) (*remote.NetVersionReply, error) {
	id, err := s.eth.NetVersion()
	if err != nil {
		return &remote.NetVersionReply{}, err
	}
	return &remote.NetVersionReply{Id: id}, nil
}

func (s *EthBackendServer) NetPeerCount(_ context.Context, _ *remote.NetPeerCountRequest) (*remote.NetPeerCountReply, error) {
	id, err := s.eth.NetPeerCount()
	if err != nil {
		return &remote.NetPeerCountReply{}, err
	}
	return &remote.NetPeerCountReply{Count: id}, nil
}

func (s *EthBackendServer) Subscribe(r *remote.SubscribeRequest, subscribeServer remote.ETHBACKEND_SubscribeServer) (err error) {
	log.Trace("Establishing event subscription channel with the RPC daemon ...")
	ch, clean := s.events.AddHeaderSubscription()
	defer clean()
	newSnCh, newSnClean := s.events.AddNewSnapshotSubscription()
	defer newSnClean()
	log.Info("new subscription to newHeaders established")
	defer func() {
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn("subscription to newHeaders closed", "reason", err)
			}
		} else {
			log.Warn("subscription to newHeaders closed")
		}
	}()
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-subscribeServer.Context().Done():
			return subscribeServer.Context().Err()
		case headersRlp := <-ch:
			for _, headerRlp := range headersRlp {
				if err = subscribeServer.Send(&remote.SubscribeReply{
					Type: remote.Event_HEADER,
					Data: headerRlp,
				}); err != nil {
					return err
				}
			}
		case <-newSnCh:
			if err = subscribeServer.Send(&remote.SubscribeReply{Type: remote.Event_NEW_SNAPSHOT}); err != nil {
				return err
			}
		}
	}
}

func (s *EthBackendServer) ProtocolVersion(_ context.Context, _ *remote.ProtocolVersionRequest) (*remote.ProtocolVersionReply, error) {
	// Hardcoding to avoid import cycle
	return &remote.ProtocolVersionReply{Id: 66}, nil
}

func (s *EthBackendServer) ClientVersion(_ context.Context, _ *remote.ClientVersionRequest) (*remote.ClientVersionReply, error) {
	return &remote.ClientVersionReply{NodeName: common.MakeName("erigon", params.Version)}, nil
}

func (s *EthBackendServer) TxnLookup(ctx context.Context, req *remote.TxnLookupRequest) (*remote.TxnLookupReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, ok, err := s.blockReader.TxnLookup(ctx, tx, gointerfaces.ConvertH256ToHash(req.TxnHash))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &remote.TxnLookupReply{BlockNumber: blockNum}, nil
}

func (s *EthBackendServer) Block(ctx context.Context, req *remote.BlockRequest) (*remote.BlockReply, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, senders, err := s.blockReader.BlockWithSenders(ctx, tx, gointerfaces.ConvertH256ToHash(req.BlockHash), req.BlockHeight)
	if err != nil {
		return nil, err
	}
	blockRlp, err := rlp.EncodeToBytes(block)
	if err != nil {
		return nil, err
	}
	sendersBytes := make([]byte, 20*len(senders))
	for i, sender := range senders {
		copy(sendersBytes[i*20:], sender[:])
	}
	return &remote.BlockReply{BlockRlp: blockRlp, Senders: sendersBytes}, nil
}

func convertPayloadStatus(payloadStatus *PayloadStatus) *remote.EnginePayloadStatus {
	reply := remote.EnginePayloadStatus{Status: payloadStatus.Status}
	if payloadStatus.LatestValidHash != (common.Hash{}) {
		reply.LatestValidHash = gointerfaces.ConvertHashToH256(payloadStatus.LatestValidHash)
	}
	if payloadStatus.ValidationError != nil {
		reply.ValidationError = payloadStatus.ValidationError.Error()
	}
	return &reply
}

func (s *EthBackendServer) stageLoopIsBusy() bool {
	for i := 0; i < 20; i++ {
		if !s.requestList.IsWaiting() {
			// This might happen, for example, in the following scenario:
			// 1) CL sends NewPayload and immediately after that ForkChoiceUpdated.
			// 2) We happily process NewPayload and stage loop is at the end.
			// 3) We start processing ForkChoiceUpdated,
			// but the stage loop hasn't moved yet from the end to the beginning of HeadersPOS
			// and thus requestList.WaitForRequest() is not called yet.

			// TODO(yperbasis): find a more elegant solution
			time.Sleep(5 * time.Millisecond)
		}
	}
	return !s.requestList.IsWaiting()
}

// EngineNewPayloadV1 validates and possibly executes payload
func (s *EthBackendServer) EngineNewPayloadV1(ctx context.Context, req *types2.ExecutionPayload) (*remote.EnginePayloadStatus, error) {
	log.Trace("[NewPayload] acquiring lock")
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()
	log.Trace("[NewPayload] lock acquired")

	if s.config.TerminalTotalDifficulty == nil {
		log.Error("[NewPayload] not a proof-of-stake chain")
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	var baseFee *big.Int
	eip1559 := false

	if req.BaseFeePerGas != nil {
		baseFee = gointerfaces.ConvertH256ToUint256Int(req.BaseFeePerGas).ToBig()
		eip1559 = true
	}

	header := types.Header{
		ParentHash:  gointerfaces.ConvertH256ToHash(req.ParentHash),
		Coinbase:    gointerfaces.ConvertH160toAddress(req.Coinbase),
		Root:        gointerfaces.ConvertH256ToHash(req.StateRoot),
		Bloom:       gointerfaces.ConvertH2048ToBloom(req.LogsBloom),
		Eip1559:     eip1559,
		BaseFee:     baseFee,
		Extra:       req.ExtraData,
		Number:      big.NewInt(int64(req.BlockNumber)),
		GasUsed:     req.GasUsed,
		GasLimit:    req.GasLimit,
		Time:        req.Timestamp,
		MixDigest:   gointerfaces.ConvertH256ToHash(req.PrevRandao),
		UncleHash:   types.EmptyUncleHash,
		Difficulty:  serenity.SerenityDifficulty,
		Nonce:       serenity.SerenityNonce,
		ReceiptHash: gointerfaces.ConvertH256ToHash(req.ReceiptRoot),
		TxHash:      types.DeriveSha(types.RawTransactions(req.Transactions)),
	}

	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
	if header.Hash() != blockHash {
		log.Error("[NewPayload] invalid block hash", "stated", common.Hash(blockHash), "actual", header.Hash())
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID_BLOCK_HASH}, nil
	}

	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	parentTd, err := rawdb.ReadTd(tx, header.ParentHash, req.BlockNumber-1)
	if err != nil {
		return nil, err
	}
	if parentTd != nil && parentTd.Cmp(s.config.TerminalTotalDifficulty) < 0 {
		log.Warn("[NewPayload] TTD not reached yet", "height", header.Number, "hash", common.Hash(blockHash))
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID_TERMINAL_BLOCK}, nil
	}
	tx.Rollback()

	// If another payload is already commissioned then we just reply with syncing
	if s.stageLoopIsBusy() {
		// We are still syncing a commissioned payload
		// TODO(yperbasis): not entirely correct since per the spec:
		// The process of validating a payload on the canonical chain MUST NOT be affected by an active sync process on a side branch of the block tree.
		// For example, if side branch B is SYNCING but the requisite data for validating a payload from canonical branch A is available, client software MUST initiate the validation process.
		// https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.6/src/engine/specification.md#payload-validation
		log.Trace("[NewPayload] stage loop is busy")
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
	}

	log.Trace("[NewPayload] sending block", "height", header.Number, "hash", common.Hash(blockHash))
	s.requestList.AddPayloadRequest(&engineapi.PayloadMessage{
		Header: &header,
		Body: &types.RawBody{
			Transactions: req.Transactions,
			Uncles:       nil,
		},
	})

	payloadStatus := <-s.statusCh
	log.Trace("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return convertPayloadStatus(&payloadStatus), nil
}

// EngineGetPayloadV1 retrieves previously assembled payload (Validators only)
func (s *EthBackendServer) EngineGetPayloadV1(ctx context.Context, req *remote.EngineGetPayloadRequest) (*types2.ExecutionPayload, error) {
	// TODO(yperbasis): getPayload should stop block assembly if that's currently in fly

	log.Trace("[GetPayload] acquiring lock")
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()
	log.Trace("[GetPayload] lock acquired")

	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	payload, ok := s.pendingPayloads[req.PayloadId]
	if !ok {
		return nil, &UnknownPayload
	}

	// getPayload should stop the build process
	// https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.7/src/engine/specification.md#payload-building
	payload.built = true

	block := payload.block

	var baseFeeReply *types2.H256
	if block.Header().BaseFee != nil {
		var baseFee uint256.Int
		baseFee.SetFromBig(block.Header().BaseFee)
		baseFeeReply = gointerfaces.ConvertUint256IntToH256(&baseFee)
	}

	encodedTransactions, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, err
	}

	return &types2.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(block.Header().ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(block.Header().Coinbase),
		Timestamp:     block.Header().Time,
		PrevRandao:    gointerfaces.ConvertHashToH256(block.Header().MixDigest),
		StateRoot:     gointerfaces.ConvertHashToH256(block.Root()),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(block.ReceiptHash()),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(block.Bloom().Bytes()),
		GasLimit:      block.GasLimit(),
		GasUsed:       block.GasUsed(),
		BlockNumber:   block.NumberU64(),
		ExtraData:     block.Extra(),
		BaseFeePerGas: baseFeeReply,
		BlockHash:     gointerfaces.ConvertHashToH256(block.Header().Hash()),
		Transactions:  encodedTransactions,
	}, nil
}

// EngineForkChoiceUpdatedV1 either states new block head or request the assembling of a new block
func (s *EthBackendServer) EngineForkChoiceUpdatedV1(ctx context.Context, req *remote.EngineForkChoiceUpdatedRequest) (*remote.EngineForkChoiceUpdatedReply, error) {
	log.Trace("[ForkChoiceUpdated] acquiring lock")
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()
	log.Trace("[ForkChoiceUpdated] lock acquired")

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	forkChoice := engineapi.ForkChoiceMessage{
		HeadBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.HeadBlockHash),
		SafeBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.SafeBlockHash),
		FinalizedBlockHash: gointerfaces.ConvertH256ToHash(req.ForkchoiceState.FinalizedBlockHash),
	}

	tx1, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	td, err := rawdb.ReadTdByHash(tx1, forkChoice.HeadBlockHash)
	if err != nil {
		return nil, err
	}
	if td != nil && td.Cmp(s.config.TerminalTotalDifficulty) < 0 {
		log.Warn("[ForkChoiceUpdated] TTD not reached yet", "forkChoice", forkChoice)
		return &remote.EngineForkChoiceUpdatedReply{
			PayloadStatus: &remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID_TERMINAL_BLOCK},
		}, nil
	}
	tx1.Rollback()

	// TODO(yperbasis): Client software MAY skip an update of the forkchoice state and
	// MUST NOT begin a payload build process if forkchoiceState.headBlockHash doesn't reference a leaf of the block tree
	// (i.e. it references an old block).
	// https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.6/src/engine/specification.md#specification-1

	if s.stageLoopIsBusy() {
		log.Trace("[ForkChoiceUpdated] stage loop is busy")
		return &remote.EngineForkChoiceUpdatedReply{
			PayloadStatus: &remote.EnginePayloadStatus{Status: remote.EngineStatus_SYNCING},
		}, nil
	}

	log.Trace("[ForkChoiceUpdated] sending forkChoiceMessage", "head", forkChoice.HeadBlockHash)
	s.requestList.AddForkChoiceRequest(&forkChoice)

	payloadStatus := <-s.statusCh
	log.Trace("[ForkChoiceUpdated] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	// No need for payload building
	if req.PayloadAttributes == nil || payloadStatus.Status != remote.EngineStatus_VALID {
		return &remote.EngineForkChoiceUpdatedReply{PayloadStatus: convertPayloadStatus(&payloadStatus)}, nil
	}

	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	s.evictOldPendingPayloads()

	// payload IDs start from 1 (0 signifies null)
	s.payloadId++

	tx2, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	headHash := rawdb.ReadHeadBlockHash(tx2)
	headNumber := rawdb.ReadHeaderNumber(tx2, headHash)
	headHeader := rawdb.ReadHeader(tx2, headHash, *headNumber)
	tx2.Rollback()

	if headHeader.Hash() != forkChoice.HeadBlockHash {
		return nil, fmt.Errorf("unexpected head hash: %x vs %x", headHeader.Hash(), forkChoice.HeadBlockHash)
	}

	emptyHeader := core.MakeEmptyHeader(headHeader, s.config, req.PayloadAttributes.Timestamp, nil)
	emptyHeader.Coinbase = gointerfaces.ConvertH160toAddress(req.PayloadAttributes.SuggestedFeeRecipient)
	emptyHeader.MixDigest = gointerfaces.ConvertH256ToHash(req.PayloadAttributes.PrevRandao)

	s.pendingPayloads[s.payloadId] = &pendingPayload{block: types.NewBlock(emptyHeader, nil, nil, nil)}

	log.Trace("[ForkChoiceUpdated] unpause assemble process")
	s.syncCond.Broadcast()

	// successfully assembled the payload and assigned the correct id
	return &remote.EngineForkChoiceUpdatedReply{
		PayloadStatus: &remote.EnginePayloadStatus{Status: remote.EngineStatus_VALID},
		PayloadId:     s.payloadId,
	}, nil
}

func (s *EthBackendServer) evictOldPendingPayloads() {
	// sort payload IDs in ascending order
	ids := make([]uint64, 0, len(s.pendingPayloads))
	for id := range s.pendingPayloads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// remove old payloads so that at most MaxPendingPayloads - 1 remain
	for i := 0; i <= len(s.pendingPayloads)-MaxPendingPayloads; i++ {
		delete(s.pendingPayloads, ids[i])
	}
}

func (s *EthBackendServer) StartProposer() {
	go func() {
		log.Trace("[Proposer] acquiring lock")
		s.syncCond.L.Lock()
		defer s.syncCond.L.Unlock()
		log.Trace("[Proposer] lock acquired")

		for {
			var blockToBuild *types.Block
			var payloadId uint64

		FindPayloadToBuild:
			for {
				if s.shutdown {
					return
				}

				tx, err := s.db.BeginRo(s.ctx)
				if err != nil {
					log.Error("Error while opening txn in block proposer", "err", err.Error())
					return
				}
				headHash := rawdb.ReadHeadBlockHash(tx)
				tx.Rollback()

				for id, payload := range s.pendingPayloads {
					if !payload.built && payload.block.ParentHash() == headHash {
						blockToBuild = payload.block
						payloadId = id
						break FindPayloadToBuild
					}
				}

				log.Trace("[Proposer] Wait until we have to process new payloads")
				s.syncCond.Wait()
				log.Trace("[Proposer] Wait finished")
			}

			// Tell the stage headers to leave space for the write transaction for mining stages
			s.requestList.Interrupt(engineapi.Yield)

			param := core.BlockProposerParametersPOS{
				ParentHash:            blockToBuild.ParentHash(),
				Timestamp:             blockToBuild.Header().Time,
				PrevRandao:            blockToBuild.MixDigest(),
				SuggestedFeeRecipient: blockToBuild.Header().Coinbase,
			}

			log.Trace("[Proposer] starting assembling...")
			s.syncCond.L.Unlock()
			block, err := s.assemblePayloadPOS(&param)
			s.syncCond.L.Lock()
			log.Trace("[Proposer] payload assembled")

			if err != nil {
				log.Warn("Error during block assembling", "err", err.Error())
			} else {
				payload, ok := s.pendingPayloads[payloadId]
				if ok && !payload.built { // don't update after engine_getPayload was called
					payload.block = block
					payload.built = true
				}
			}
		}
	}()
}

func (s *EthBackendServer) StopProposer() {
	log.Trace("[StopProposer] acquiring lock")
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()
	log.Trace("[StopProposer] lock acquired")

	s.shutdown = true
	s.syncCond.Broadcast()
}

func (s *EthBackendServer) NodeInfo(_ context.Context, r *remote.NodesInfoRequest) (*remote.NodesInfoReply, error) {
	nodesInfo, err := s.eth.NodesInfo(int(r.Limit))
	if err != nil {
		return nil, err
	}
	return nodesInfo, nil
}

func (s *EthBackendServer) Peers(ctx context.Context, _ *emptypb.Empty) (*remote.PeersReply, error) {
	return s.eth.Peers(ctx)
}

func (s *EthBackendServer) SubscribeLogs(server remote.ETHBACKEND_SubscribeLogsServer) (err error) {
	if s.logsFilter != nil {
		return s.logsFilter.subscribeLogs(server)
	}
	return fmt.Errorf("no logs filter available")
}
