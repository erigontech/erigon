package privateapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type assemblePayloadPOSFunc func(random common.Hash, suggestedFeeRecipient common.Address, timestamp uint64) (*types.Block, error)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 0, Patch: 0}

const MaxPendingPayloads = 128

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
	pendingPayloads map[uint64]types2.ExecutionPayload
	// Send new Beacon Chain payloads to staged sync
	newPayloadCh chan<- PayloadMessage
	// Send Beacon Chain fork choice updates to staged sync
	forkChoiceCh chan<- ForkChoiceMessage
	// Replies to newPayload & forkchoice requests
	statusCh <-chan PayloadStatus
	// Determines whether stageloop is processing a block or not
	waitingForBeaconChain *uint32       // atomic boolean flag
	skipCycleHack         chan struct{} // with this channel we tell the stagedsync that we want to assemble a block
	assemblePayloadPOS    assemblePayloadPOSFunc
	proposing             bool
	syncCond              *sync.Cond // Engine API is asynchronous, we want to avoid CL to call different APIs at the same time
	shutdown              bool
}

type EthBackend interface {
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
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

// The message we are going to send to the stage sync in NewPayload
type PayloadMessage struct {
	Header *types.Header
	Body   *types.RawBody
}

// The message we are going to send to the stage sync in ForkchoiceUpdated
type ForkChoiceMessage struct {
	HeadBlockHash      common.Hash
	SafeBlockHash      common.Hash
	FinalizedBlockHash common.Hash
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *Events, blockReader interfaces.BlockAndTxnReader,
	config *params.ChainConfig, newPayloadCh chan<- PayloadMessage, forkChoiceCh chan<- ForkChoiceMessage, statusCh <-chan PayloadStatus,
	waitingForBeaconChain *uint32, skipCycleHack chan struct{}, assemblePayloadPOS assemblePayloadPOSFunc, proposing bool,
) *EthBackendServer {
	return &EthBackendServer{ctx: ctx, eth: eth, events: events, db: db, blockReader: blockReader, config: config,
		newPayloadCh: newPayloadCh, forkChoiceCh: forkChoiceCh, statusCh: statusCh, waitingForBeaconChain: waitingForBeaconChain,
		pendingPayloads: make(map[uint64]types2.ExecutionPayload), skipCycleHack: skipCycleHack,
		assemblePayloadPOS: assemblePayloadPOS, proposing: proposing, syncCond: sync.NewCond(&sync.Mutex{}),
	}
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
	for i := 0; i < 10; i++ {
		if atomic.LoadUint32(s.waitingForBeaconChain) == 0 {
			// This might happen, for example, in the following scenario:
			// 1) CL sends NewPayload and immediately after that ForkChoiceUpdated
			// 2) We happily process NewPayload and stage loop is at the end
			// 3) We start processing ForkChoiceUpdated,
			// but the stage looped hasn't yet moved from the end to the beginning of HeadersPOS
			// and thus waitingForBeaconChain is not set yet.

			// TODO(yperbasis): find a more elegant solution
			time.Sleep(time.Millisecond)
		}
	}
	return atomic.LoadUint32(s.waitingForBeaconChain) == 0
}

// EngineNewPayloadV1, validates and possibly executes payload
func (s *EthBackendServer) EngineNewPayloadV1(ctx context.Context, req *types2.ExecutionPayload) (*remote.EnginePayloadStatus, error) {
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()

	if s.config.TerminalTotalDifficulty == nil {
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
		MixDigest:   gointerfaces.ConvertH256ToHash(req.Random),
		UncleHash:   types.EmptyUncleHash,
		Difficulty:  serenity.SerenityDifficulty,
		Nonce:       serenity.SerenityNonce,
		ReceiptHash: gointerfaces.ConvertH256ToHash(req.ReceiptRoot),
		TxHash:      types.DeriveSha(types.RawTransactions(req.Transactions)), // TODO(yperbasis): prohibit EIP-2718 txn wrapped as RLP strings
	}

	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
	if header.Hash() != blockHash {
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID_BLOCK_HASH}, nil
	}

	// If another payload is already commissioned then we just reply with syncing
	if s.stageLoopIsBusy() {
		// We are still syncing a commissioned payload
		// TODO(yperbasis): not entirely correct since per the spec:
		// The process of validating a payload on the canonical chain MUST NOT be affected by an active sync process on a side branch of the block tree.
		// For example, if side branch B is SYNCING but the requisite data for validating a payload from canonical branch A is available, client software MUST initiate the validation process.
		// https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.6/src/engine/specification.md#payload-validation
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
	}

	// Send the block over
	s.newPayloadCh <- PayloadMessage{
		Header: &header,
		Body: &types.RawBody{
			Transactions: req.Transactions,
			Uncles:       nil,
		},
	}

	payloadStatus := <-s.statusCh
	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return convertPayloadStatus(&payloadStatus), nil
}

// EngineGetPayloadV1, retrieves previously assembled payload (Validators only)
func (s *EthBackendServer) EngineGetPayloadV1(ctx context.Context, req *remote.EngineGetPayloadRequest) (*types2.ExecutionPayload, error) {
	// TODO(yperbasis): getPayload should stop block assembly if that's currently in fly

	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()

	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	for {
		payload, ok := s.pendingPayloads[req.PayloadId]
		if !ok {
			return nil, fmt.Errorf("unknown payload")
		}

		if payload.BlockNumber != 0 {
			return &payload, nil
		}

		// Wait for payloads assembling thread to finish
		s.syncCond.Wait()
	}
}

// EngineForkChoiceUpdatedV1, either states new block head or request the assembling of a new block
func (s *EthBackendServer) EngineForkChoiceUpdatedV1(ctx context.Context, req *remote.EngineForkChoiceUpdatedRequest) (*remote.EngineForkChoiceUpdatedReply, error) {
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	// TODO(yperbasis): Client software MAY skip an update of the forkchoice state and
	// MUST NOT begin a payload build process if forkchoiceState.headBlockHash doesn't reference a leaf of the block tree
	// (i.e. it references an old block).
	// https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.6/src/engine/specification.md#specification-1

	if s.stageLoopIsBusy() {
		return &remote.EngineForkChoiceUpdatedReply{
			PayloadStatus: &remote.EnginePayloadStatus{Status: remote.EngineStatus_SYNCING},
		}, nil
	}

	s.forkChoiceCh <- ForkChoiceMessage{
		HeadBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.HeadBlockHash),
		SafeBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.SafeBlockHash),
		FinalizedBlockHash: gointerfaces.ConvertH256ToHash(req.ForkchoiceState.FinalizedBlockHash),
	}

	payloadStatus := <-s.statusCh
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

	s.pendingPayloads[s.payloadId] = types2.ExecutionPayload{
		Random:    req.PayloadAttributes.Random,
		Timestamp: req.PayloadAttributes.Timestamp,
		Coinbase:  req.PayloadAttributes.SuggestedFeeRecipient,
	}
	// Unpause assemble process
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
		s.syncCond.L.Lock()
		defer s.syncCond.L.Unlock()

		for {
			// Wait until we have to process new payloads
			s.syncCond.Wait()

			if s.shutdown {
				return
			}

			// Go over each payload and re-update them
			for id := range s.pendingPayloads {
				// If we already assembled this block, let's just skip it
				if s.pendingPayloads[id].BlockNumber != 0 {
					continue
				}
				// we do not want to make a copy of the payload in the loop because it contains a lock
				random := gointerfaces.ConvertH256ToHash(s.pendingPayloads[id].Random)
				coinbase := gointerfaces.ConvertH160toAddress(s.pendingPayloads[id].Coinbase)
				timestamp := s.pendingPayloads[id].Timestamp
				// Tell the stage headers to leave space for the write transaction for mining stages
				s.skipCycleHack <- struct{}{}

				block, err := s.assemblePayloadPOS(random, coinbase, timestamp)
				if err != nil {
					log.Warn("Error during block assembling", "err", err.Error())
					return
				}
				var baseFeeReply *types2.H256
				if block.Header().BaseFee != nil {
					var baseFee uint256.Int
					baseFee.SetFromBig(block.Header().BaseFee)
					baseFeeReply = gointerfaces.ConvertUint256IntToH256(&baseFee)
				}
				var encodedTransactions [][]byte
				buf := bytes.NewBuffer(nil)

				for _, tx := range block.Transactions() {
					buf.Reset()
					// EIP-2718 txn shouldn't be additionally wrapped as RLP strings,
					// so MarshalBinary instead of rlp.Encode
					err := tx.MarshalBinary(buf)
					if err != nil {
						log.Warn("Failed to marshal transaction", "err", err.Error())
						return
					}
					encodedTransactions = append(encodedTransactions, common.CopyBytes(buf.Bytes()))
				}
				// Set parameters accordingly to what the beacon chain told us and from what the mining stage told us
				s.pendingPayloads[id] = types2.ExecutionPayload{
					ParentHash:    gointerfaces.ConvertHashToH256(block.Header().ParentHash),
					Coinbase:      gointerfaces.ConvertAddressToH160(block.Header().Coinbase),
					Timestamp:     s.pendingPayloads[id].Timestamp,
					Random:        s.pendingPayloads[id].Random,
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
				}
			}

			// Broadcast the signal that an entire loop over pending payloads has been executed
			s.syncCond.Broadcast()
		}
	}()
}

func (s *EthBackendServer) StopProposer() {
	s.syncCond.L.Lock()
	defer s.syncCond.L.Unlock()

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
