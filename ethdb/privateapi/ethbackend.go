package privateapi

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/emptypb"
)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
// 3.1.0 - add Subscribe to logs
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 1, Patch: 0}

const MaxBuilders = 128

var UnknownPayloadErr = rpc.CustomError{Code: -38001, Message: "Unknown payload"}
var InvalidForkchoiceStateErr = rpc.CustomError{Code: -38002, Message: "Invalid forkchoice state"}
var InvalidPayloadAttributesErr = rpc.CustomError{Code: -38003, Message: "Invalid payload attributes"}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx         context.Context
	eth         EthBackend
	events      *Events
	db          kv.RoDB
	blockReader services.BlockAndTxnReader
	config      *params.ChainConfig
	// Block proposing for proof-of-stake
	payloadId uint64
	builders  map[uint64]*builder.BlockBuilder

	builderFunc builder.BlockBuilderFunc
	proposing   bool
	lock        sync.Mutex // Engine API is asynchronous, we want to avoid CL to call different APIs at the same time
	logsFilter  *LogsFilterAggregator
	hd          *headerdownload.HeaderDownload
}

type EthBackend interface {
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
	Peers(ctx context.Context) (*remote.PeersReply, error)
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *Events, blockReader services.BlockAndTxnReader,
	config *params.ChainConfig, builderFunc builder.BlockBuilderFunc, hd *headerdownload.HeaderDownload, proposing bool,
) *EthBackendServer {
	s := &EthBackendServer{ctx: ctx, eth: eth, events: events, db: db, blockReader: blockReader, config: config,
		builders:    make(map[uint64]*builder.BlockBuilder),
		builderFunc: builderFunc, proposing: proposing, logsFilter: NewLogsFilterAggregator(events), hd: hd,
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
	log.Debug("Establishing event subscription channel with the RPC daemon ...")
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
	_ = subscribeServer.Send(&remote.SubscribeReply{Type: remote.Event_NEW_SNAPSHOT})
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
		// Not a perfect solution, assumes there are no transactions in block 0
		return &remote.TxnLookupReply{BlockNumber: 0}, nil
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

func convertPayloadStatus(payloadStatus *engineapi.PayloadStatus) *remote.EnginePayloadStatus {
	reply := remote.EnginePayloadStatus{Status: payloadStatus.Status}
	if payloadStatus.Status != remote.EngineStatus_SYNCING {
		reply.LatestValidHash = gointerfaces.ConvertHashToH256(payloadStatus.LatestValidHash)
	}
	if payloadStatus.ValidationError != nil {
		reply.ValidationError = payloadStatus.ValidationError.Error()
	}
	return &reply
}

func (s *EthBackendServer) stageLoopIsBusy() bool {
	for i := 0; i < 20; i++ {
		if !s.hd.BeaconRequestList.IsWaiting() {
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
	return !s.hd.BeaconRequestList.IsWaiting()
}

// EngineNewPayloadV1 validates and possibly executes payload
func (s *EthBackendServer) EngineNewPayloadV1(ctx context.Context, req *types2.ExecutionPayload) (*remote.EnginePayloadStatus, error) {
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
		TxHash:      types.DeriveSha(types.BinaryTransactions(req.Transactions)),
	}

	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
	if header.Hash() != blockHash {
		log.Error("[NewPayload] invalid block hash", "stated", common.Hash(blockHash), "actual", header.Hash())
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID_BLOCK_HASH}, nil
	}

	for _, txn := range req.Transactions {
		if types.TypedTransactionMarshalledAsRlpString(txn) {
			log.Warn("[NewPayload] typed txn marshalled as RLP string", "txn", common.Bytes2Hex(txn))
			return &remote.EnginePayloadStatus{
				Status:          remote.EngineStatus_INVALID,
				LatestValidHash: nil,
				ValidationError: "typed txn marshalled as RLP string",
			}, nil
		}
	}

	transactions, err := types.DecodeTransactions(req.Transactions)
	if err != nil {
		log.Warn("[NewPayload] failed to decode transactions", "err", err)
		return &remote.EnginePayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			LatestValidHash: nil,
			ValidationError: err.Error(),
		}, nil
	}
	block := types.NewBlockFromStorage(blockHash, &header, transactions, nil)

	possibleStatus, err := s.getPayloadStatusFromHashIfPossible(blockHash, req.BlockNumber, header.ParentHash, true)
	if err != nil {
		return nil, err
	}
	if possibleStatus != nil {
		return convertPayloadStatus(possibleStatus), nil
	}
	// If another payload is already commissioned then we just reply with syncing
	if s.stageLoopIsBusy() {
		// We are still syncing a commissioned payload
		// TODO(yperbasis): not entirely correct since per the spec:
		// The process of validating a payload on the canonical chain MUST NOT be affected by an active sync process on a side branch of the block tree.
		// For example, if side branch B is SYNCING but the requisite data for validating a payload from canonical branch A is available, client software MUST initiate the validation process.
		// https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.6/src/engine/specification.md#payload-validation
		log.Debug("[NewPayload] stage loop is busy")
		return &remote.EnginePayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Debug("[NewPayload] sending block", "height", header.Number, "hash", common.Hash(blockHash))
	s.hd.BeaconRequestList.AddPayloadRequest(block)

	payloadStatus := <-s.hd.PayloadStatusCh
	log.Debug("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return convertPayloadStatus(&payloadStatus), nil
}

// Check if we can make out a status from the payload hash/head hash.
func (s *EthBackendServer) getPayloadStatusFromHashIfPossible(blockHash common.Hash, blockNumber uint64, parentHash common.Hash, newPayload bool) (*engineapi.PayloadStatus, error) {
	// Determine which prefix to use for logs
	var prefix string
	if newPayload {
		prefix = "NewPayload"
	} else {
		prefix = "ForkChoiceUpdated"
	}
	if s.config.TerminalTotalDifficulty == nil {
		log.Error(fmt.Sprintf("[%s] not a proof-of-stake chain", prefix))
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	if s.hd == nil {
		return nil, nil
	}

	tx, err := s.db.BeginRo(s.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}
	// Retrieve parent and total difficulty.
	var parent *types.Header
	var td *big.Int
	if newPayload {
		// Obtain TD
		parent, err = rawdb.ReadHeaderByHash(tx, parentHash)
		if err != nil {
			return nil, err
		}
		td, err = rawdb.ReadTdByHash(tx, parentHash)
	} else {
		td, err = rawdb.ReadTdByHash(tx, blockHash)
	}
	if err != nil {
		return nil, err
	}
	// Check if we already reached TTD.
	if td != nil && td.Cmp(s.config.TerminalTotalDifficulty) < 0 {
		log.Warn(fmt.Sprintf("[%s] TTD not reached yet", prefix), "hash", common.Hash(blockHash))
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_INVALID, LatestValidHash: common.Hash{}}, nil
	}

	var canonicalHash common.Hash
	if header != nil {
		canonicalHash, err = rawdb.ReadCanonicalHash(tx, header.Number.Uint64())
	}
	if err != nil {
		return nil, err
	}

	if newPayload && parent != nil && blockNumber != parent.Number.Uint64()+1 {
		log.Warn(fmt.Sprintf("[%s] Invalid block number", prefix), "headerNumber", blockNumber, "parentNumber", parent.Number.Uint64())
		s.hd.ReportBadHeaderPoS(blockHash, parent.Hash())
		return &engineapi.PayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			LatestValidHash: parent.Hash(),
			ValidationError: errors.New("invalid block number"),
		}, nil
	}
	// Check if we already determined if the hash is attributed to a previously received invalid header.
	bad, lastValidHash := s.hd.IsBadHeaderPoS(blockHash)
	if bad {
		log.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash)
	} else if newPayload {
		bad, lastValidHash = s.hd.IsBadHeaderPoS(parentHash)
		if bad {
			log.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash, "parentHash", parentHash)
		}
	}
	if bad {
		s.hd.ReportBadHeaderPoS(blockHash, lastValidHash)
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_INVALID, LatestValidHash: lastValidHash}, nil
	}

	// If header is already validated or has a missing parent, you can either return VALID or SYNCING.
	if newPayload {
		if header != nil && canonicalHash == blockHash {
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_VALID, LatestValidHash: blockHash}, nil
		}

		if parent == nil && s.hd.PosStatus() == headerdownload.Syncing {
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
		}

		return nil, nil
	}

	if header == nil {
		if s.hd.PosStatus() == headerdownload.Syncing {
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil

		}
		return nil, nil
	}

	headHash := rawdb.ReadHeadBlockHash(tx)
	if err != nil {
		return nil, err
	}

	// We add the extra restriction blockHash != headHash for the FCU case of canonicalHash == blockHash
	// because otherwise (when FCU points to the head) we want go to stage headers
	// so that it calls writeForkChoiceHashes.
	if blockHash != headHash && canonicalHash == blockHash {
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_VALID, LatestValidHash: blockHash}, nil
	}

	return nil, nil
}

// EngineGetPayloadV1 retrieves previously assembled payload (Validators only)
func (s *EthBackendServer) EngineGetPayloadV1(ctx context.Context, req *remote.EngineGetPayloadRequest) (*types2.ExecutionPayload, error) {
	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	log.Debug("[GetPayload] acquiring lock")
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Debug("[GetPayload] lock acquired")

	builder, ok := s.builders[req.PayloadId]
	if !ok {
		log.Warn("Payload not stored", "payloadId", req.PayloadId)
		return nil, &UnknownPayloadErr
	}

	block := builder.Stop()

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
	log.Info("Block request successful", "hash", block.Header().Hash(), "transactions count", len(encodedTransactions), "number", block.NumberU64())

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
	forkChoice := engineapi.ForkChoiceMessage{
		HeadBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.HeadBlockHash),
		SafeBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.SafeBlockHash),
		FinalizedBlockHash: gointerfaces.ConvertH256ToHash(req.ForkchoiceState.FinalizedBlockHash),
	}

	status, err := s.getPayloadStatusFromHashIfPossible(forkChoice.HeadBlockHash, 0, common.Hash{}, false)
	if err != nil {
		return nil, err
	}

	if status == nil && s.stageLoopIsBusy() {
		log.Debug("[ForkChoiceUpdated] stage loop is busy")
		return &remote.EngineForkChoiceUpdatedReply{
			PayloadStatus: &remote.EnginePayloadStatus{Status: remote.EngineStatus_SYNCING},
		}, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if status == nil {
		log.Debug("[ForkChoiceUpdated] sending forkChoiceMessage", "head", forkChoice.HeadBlockHash)
		s.hd.BeaconRequestList.AddForkChoiceRequest(&forkChoice)

		statusDeref := <-s.hd.PayloadStatusCh
		status = &statusDeref
		log.Debug("[ForkChoiceUpdated] got reply", "payloadStatus", status)

		if status.CriticalError != nil {
			return nil, status.CriticalError
		}
	}

	// No need for payload building
	if req.PayloadAttributes == nil || status.Status != remote.EngineStatus_VALID {
		return &remote.EngineForkChoiceUpdatedReply{PayloadStatus: convertPayloadStatus(status)}, nil
	}

	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	tx2, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx2.Rollback()
	headHash := rawdb.ReadHeadBlockHash(tx2)
	headNumber := rawdb.ReadHeaderNumber(tx2, headHash)
	headHeader := rawdb.ReadHeader(tx2, headHash, *headNumber)
	tx2.Rollback()

	if headHeader.Hash() != forkChoice.HeadBlockHash {
		// Per https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.9/src/engine/specification.md#specification-1:
		// Client software MAY skip an update of the forkchoice state and
		// MUST NOT begin a payload build process if forkchoiceState.headBlockHash doesn't reference a leaf of the block tree.
		// That is, the block referenced by forkchoiceState.headBlockHash is neither the head of the canonical chain nor a block at the tip of any other chain.
		// In the case of such an event, client software MUST return
		// {payloadStatus: {status: VALID, latestValidHash: forkchoiceState.headBlockHash, validationError: null}, payloadId: null}.

		log.Warn("Skipping payload building because forkchoiceState.headBlockHash is not the head of the canonical chain",
			"forkChoice.HeadBlockHash", forkChoice.HeadBlockHash, "headHeader.Hash", headHeader.Hash())
		return &remote.EngineForkChoiceUpdatedReply{PayloadStatus: convertPayloadStatus(status)}, nil
	}

	if headHeader.Time >= req.PayloadAttributes.Timestamp {
		return nil, &InvalidPayloadAttributesErr
	}

	// Initiate payload building

	s.evictOldBuilders()

	// payload IDs start from 1 (0 signifies null)
	s.payloadId++

	emptyHeader := core.MakeEmptyHeader(headHeader, s.config, req.PayloadAttributes.Timestamp, nil)
	emptyHeader.Coinbase = gointerfaces.ConvertH160toAddress(req.PayloadAttributes.SuggestedFeeRecipient)
	emptyHeader.MixDigest = gointerfaces.ConvertH256ToHash(req.PayloadAttributes.PrevRandao)

	param := core.BlockBuilderParameters{
		ParentHash:            forkChoice.HeadBlockHash,
		Timestamp:             req.PayloadAttributes.Timestamp,
		PrevRandao:            emptyHeader.MixDigest,
		SuggestedFeeRecipient: emptyHeader.Coinbase,
	}

	s.builders[s.payloadId] = builder.NewBlockBuilder(s.builderFunc, &param, emptyHeader)

	return &remote.EngineForkChoiceUpdatedReply{
		PayloadStatus: &remote.EnginePayloadStatus{
			Status:          remote.EngineStatus_VALID,
			LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		},
		PayloadId: s.payloadId,
	}, nil
}

func (s *EthBackendServer) evictOldBuilders() {
	// sort payload IDs in ascending order
	ids := make([]uint64, 0, len(s.builders))
	for id := range s.builders {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(s.builders)-MaxBuilders; i++ {
		delete(s.builders, ids[i])
	}
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
