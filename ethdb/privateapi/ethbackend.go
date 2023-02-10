package privateapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
// 3.1.0 - add Subscribe to logs
// 3.2.0 - add EngineGetBlobsBundleV1
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 2, Patch: 0}

const MaxBuilders = 128

var UnknownPayloadErr = rpc.CustomError{Code: -38001, Message: "Unknown payload"}
var InvalidForkchoiceStateErr = rpc.CustomError{Code: -38002, Message: "Invalid forkchoice state"}
var InvalidPayloadAttributesErr = rpc.CustomError{Code: -38003, Message: "Invalid payload attributes"}
var TooLargeRequestErr = rpc.CustomError{Code: -38004, Message: "Too large request"}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx         context.Context
	eth         EthBackend
	events      *shards.Events
	db          kv.RoDB
	blockReader services.BlockAndTxnReader
	config      *chain.Config
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
	Etherbase() (libcommon.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
	Peers(ctx context.Context) (*remote.PeersReply, error)
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *shards.Events, blockReader services.BlockAndTxnReader,
	config *chain.Config, builderFunc builder.BlockBuilderFunc, hd *headerdownload.HeaderDownload, proposing bool,
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
	out := &remote.EtherbaseReply{Address: gointerfaces.ConvertAddressToH160(libcommon.Address{})}

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

func (s *EthBackendServer) PendingBlock(_ context.Context, _ *emptypb.Empty) (*remote.PendingBlockReply, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	b := s.builders[s.payloadId]
	if b == nil {
		return nil, nil
	}

	pendingBlock := b.Block()
	if pendingBlock == nil {
		return nil, nil
	}

	blockRlp, err := rlp.EncodeToBytes(pendingBlock)
	if err != nil {
		return nil, err
	}

	return &remote.PendingBlockReply{BlockRlp: blockRlp}, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	wait, ok := s.hd.BeaconRequestList.WaitForWaiting(ctx)
	if !ok {
		select {
		case <-wait:
		case <-ctx.Done():
		}
	}

	return !s.hd.BeaconRequestList.IsWaiting()
}

func (s *EthBackendServer) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !s.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if s.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

func (s *EthBackendServer) EngineGetBlobsBundleV1(ctx context.Context, in *remote.EngineGetBlobsBundleRequest) (*types2.BlobsBundleV1, error) {
	return nil, fmt.Errorf("EngineGetBlobsBundleV1: not implemented yet")
}

// EngineNewPayload validates and possibly executes payload
func (s *EthBackendServer) EngineNewPayload(ctx context.Context, req *types2.ExecutionPayload) (*remote.EnginePayloadStatus, error) {
	header := types.Header{
		ParentHash:  gointerfaces.ConvertH256ToHash(req.ParentHash),
		Coinbase:    gointerfaces.ConvertH160toAddress(req.Coinbase),
		Root:        gointerfaces.ConvertH256ToHash(req.StateRoot),
		Bloom:       gointerfaces.ConvertH2048ToBloom(req.LogsBloom),
		BaseFee:     gointerfaces.ConvertH256ToUint256Int(req.BaseFeePerGas).ToBig(),
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
	var withdrawals []*types.Withdrawal
	if req.Version >= 2 {
		withdrawals = ConvertWithdrawalsFromRpc(req.Withdrawals)
	}

	if withdrawals != nil {
		wh := types.DeriveSha(types.Withdrawals(withdrawals))
		header.WithdrawalsHash = &wh
	}

	if err := s.checkWithdrawalsPresence(header.Time, withdrawals); err != nil {
		return nil, err
	}

	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
	if header.Hash() != blockHash {
		log.Error("[NewPayload] invalid block hash", "stated", libcommon.Hash(blockHash), "actual", header.Hash())
		return &remote.EnginePayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			ValidationError: "invalid block hash",
		}, nil
	}

	for _, txn := range req.Transactions {
		if types.TypedTransactionMarshalledAsRlpString(txn) {
			log.Warn("[NewPayload] typed txn marshalled as RLP string", "txn", common.Bytes2Hex(txn))
			return &remote.EnginePayloadStatus{
				Status:          remote.EngineStatus_INVALID,
				ValidationError: "typed txn marshalled as RLP string",
			}, nil
		}
	}

	transactions, err := types.DecodeTransactions(req.Transactions)
	if err != nil {
		log.Warn("[NewPayload] failed to decode transactions", "err", err)
		return &remote.EnginePayloadStatus{
			Status:          remote.EngineStatus_INVALID,
			ValidationError: err.Error(),
		}, nil
	}
	block := types.NewBlockFromStorage(blockHash, &header, transactions, nil /* uncles */, withdrawals)

	possibleStatus, err := s.getQuickPayloadStatusIfPossible(blockHash, req.BlockNumber, header.ParentHash, nil, true)
	if err != nil {
		return nil, err
	}
	if possibleStatus != nil {
		return convertPayloadStatus(possibleStatus), nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	log.Debug("[NewPayload] sending block", "height", header.Number, "hash", libcommon.Hash(blockHash))
	s.hd.BeaconRequestList.AddPayloadRequest(block)

	payloadStatus := <-s.hd.PayloadStatusCh
	log.Debug("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return convertPayloadStatus(&payloadStatus), nil
}

// Check if we can quickly determine the status of a newPayload or forkchoiceUpdated.
func (s *EthBackendServer) getQuickPayloadStatusIfPossible(blockHash libcommon.Hash, blockNumber uint64, parentHash libcommon.Hash, forkchoiceMessage *engineapi.ForkChoiceMessage, newPayload bool) (*engineapi.PayloadStatus, error) {
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
		return nil, fmt.Errorf("headerdownload is nil")
	}

	tx, err := s.db.BeginRo(s.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Some Consensus layer clients sometimes sends us repeated FCUs and make Erigon print a gazillion logs.
	// E.G teku sometimes will end up spamming fcu on the terminal block if it has not synced to that point.
	if forkchoiceMessage != nil &&
		forkchoiceMessage.FinalizedBlockHash == rawdb.ReadForkchoiceFinalized(tx) &&
		forkchoiceMessage.HeadBlockHash == rawdb.ReadForkchoiceHead(tx) &&
		forkchoiceMessage.SafeBlockHash == rawdb.ReadForkchoiceSafe(tx) {
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_VALID, LatestValidHash: blockHash}, nil
	}

	header, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}
	// Retrieve parent and total difficulty.
	var parent *types.Header
	var td *big.Int
	if newPayload {
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

	if td != nil && td.Cmp(s.config.TerminalTotalDifficulty) < 0 {
		log.Warn(fmt.Sprintf("[%s] Beacon Chain request before TTD", prefix), "hash", blockHash)
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_INVALID, LatestValidHash: libcommon.Hash{}}, nil
	}

	if !s.hd.POSSync() {
		log.Info(fmt.Sprintf("[%s] Still in PoW sync", prefix), "hash", blockHash)
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
	}

	var canonicalHash libcommon.Hash
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

		if parent == nil && s.hd.PosStatus() != headerdownload.Idle {
			log.Debug(fmt.Sprintf("[%s] Downloading some other PoS blocks", prefix), "hash", blockHash)
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
		}
	} else {
		if header == nil && s.hd.PosStatus() != headerdownload.Idle {
			log.Debug(fmt.Sprintf("[%s] Downloading some other PoS stuff", prefix), "hash", blockHash)
			return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
		}
		// Following code ensures we skip the fork choice state update if if forkchoiceState.headBlockHash references an ancestor of the head of canonical chain
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
	}

	// If another payload is already commissioned then we just reply with syncing
	if s.stageLoopIsBusy() {
		log.Debug(fmt.Sprintf("[%s] stage loop is busy", prefix))
		return &engineapi.PayloadStatus{Status: remote.EngineStatus_SYNCING}, nil
	}

	return nil, nil
}

// The expected value to be received by the feeRecipient in wei
func blockValue(block *types.Block, baseFee *uint256.Int) *uint256.Int {
	blockValue := uint256.NewInt(0)
	for _, tx := range block.Transactions() {
		gas := new(uint256.Int).SetUint64(tx.GetGas())
		effectiveTip := tx.GetEffectiveGasTip(baseFee)
		txValue := new(uint256.Int).Mul(gas, effectiveTip)
		blockValue.Add(blockValue, txValue)
	}
	return blockValue
}

// EngineGetPayload retrieves previously assembled payload (Validators only)
func (s *EthBackendServer) EngineGetPayload(ctx context.Context, req *remote.EngineGetPayloadRequest) (*remote.EngineGetPayloadResponse, error) {
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

	block, err := builder.Stop()
	if err != nil {
		log.Error("Failed to build PoS block", "err", err)
		return nil, err
	}

	baseFee := new(uint256.Int)
	baseFee.SetFromBig(block.Header().BaseFee)

	encodedTransactions, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, err
	}

	payload := &types2.ExecutionPayload{
		Version:       1,
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
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(block.Header().Hash()),
		Transactions:  encodedTransactions,
	}
	if block.Withdrawals() != nil {
		payload.Version = 2
		payload.Withdrawals = ConvertWithdrawalsToRpc(block.Withdrawals())
	}

	blockValue := blockValue(block, baseFee)
	return &remote.EngineGetPayloadResponse{
		ExecutionPayload: payload,
		BlockValue:       gointerfaces.ConvertUint256IntToH256(blockValue),
	}, nil
}

// engineForkChoiceUpdated either states new block head or request the assembling of a new block
func (s *EthBackendServer) EngineForkChoiceUpdated(ctx context.Context, req *remote.EngineForkChoiceUpdatedRequest,
) (*remote.EngineForkChoiceUpdatedResponse, error) {
	forkChoice := engineapi.ForkChoiceMessage{
		HeadBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.HeadBlockHash),
		SafeBlockHash:      gointerfaces.ConvertH256ToHash(req.ForkchoiceState.SafeBlockHash),
		FinalizedBlockHash: gointerfaces.ConvertH256ToHash(req.ForkchoiceState.FinalizedBlockHash),
	}

	status, err := s.getQuickPayloadStatusIfPossible(forkChoice.HeadBlockHash, 0, libcommon.Hash{}, &forkChoice, false)
	if err != nil {
		return nil, err
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
	payloadAttributes := req.PayloadAttributes
	if payloadAttributes == nil || status.Status != remote.EngineStatus_VALID {
		return &remote.EngineForkChoiceUpdatedResponse{PayloadStatus: convertPayloadStatus(status)}, nil
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
		// Per Item 2 of https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.9/src/engine/specification.md#specification-1:
		// Client software MAY skip an update of the forkchoice state and
		// MUST NOT begin a payload build process if forkchoiceState.headBlockHash doesn't reference a leaf of the block tree.
		// That is, the block referenced by forkchoiceState.headBlockHash is neither the head of the canonical chain nor a block at the tip of any other chain.
		// In the case of such an event, client software MUST return
		// {payloadStatus: {status: VALID, latestValidHash: forkchoiceState.headBlockHash, validationError: null}, payloadId: null}.

		log.Warn("Skipping payload building because forkchoiceState.headBlockHash is not the head of the canonical chain",
			"forkChoice.HeadBlockHash", forkChoice.HeadBlockHash, "headHeader.Hash", headHeader.Hash())
		return &remote.EngineForkChoiceUpdatedResponse{PayloadStatus: convertPayloadStatus(status)}, nil
	}

	if headHeader.Time >= payloadAttributes.Timestamp {
		return nil, &InvalidPayloadAttributesErr
	}

	param := core.BlockBuilderParameters{
		ParentHash:            forkChoice.HeadBlockHash,
		Timestamp:             payloadAttributes.Timestamp,
		PrevRandao:            gointerfaces.ConvertH256ToHash(payloadAttributes.PrevRandao),
		SuggestedFeeRecipient: gointerfaces.ConvertH160toAddress(payloadAttributes.SuggestedFeeRecipient),
		PayloadId:             s.payloadId,
	}
	if payloadAttributes.Version >= 2 {
		param.Withdrawals = ConvertWithdrawalsFromRpc(payloadAttributes.Withdrawals)
	}

	if err := s.checkWithdrawalsPresence(payloadAttributes.Timestamp, param.Withdrawals); err != nil {
		return nil, err
	}

	// Initiate payload building

	s.evictOldBuilders()

	// payload IDs start from 1 (0 signifies null)
	s.payloadId++

	s.builders[s.payloadId] = builder.NewBlockBuilder(s.builderFunc, &param)
	log.Debug("BlockBuilder added", "payload", s.payloadId)

	return &remote.EngineForkChoiceUpdatedResponse{
		PayloadStatus: &remote.EnginePayloadStatus{
			Status:          remote.EngineStatus_VALID,
			LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		},
		PayloadId: s.payloadId,
	}, nil
}

func (s *EthBackendServer) EngineGetPayloadBodiesByHashV1(ctx context.Context, request *remote.EngineGetPayloadBodiesByHashV1Request) (*remote.EngineGetPayloadBodiesV1Response, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}

	bodies := make([]*types2.ExecutionPayloadBodyV1, len(request.Hashes))

	for hashIdx, hash := range request.Hashes {
		h := gointerfaces.ConvertH256ToHash(hash)
		block, err := rawdb.ReadBlockByHash(tx, h)
		if err != nil {
			return nil, err
		}

		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies[hashIdx] = body
	}

	return &remote.EngineGetPayloadBodiesV1Response{Bodies: bodies}, nil
}

func (s *EthBackendServer) EngineGetPayloadBodiesByRangeV1(ctx context.Context, request *remote.EngineGetPayloadBodiesByRangeV1Request) (*remote.EngineGetPayloadBodiesV1Response, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}

	bodies := make([]*types2.ExecutionPayloadBodyV1, 0, request.Count)

	for i := uint64(0); i < request.Count; i++ {
		hash, err := rawdb.ReadCanonicalHash(tx, request.Start+i)
		if err != nil {
			return nil, err
		}
		if hash == (libcommon.Hash{}) {
			// break early if beyond the last known canonical header
			break
		}

		block := rawdb.ReadBlock(tx, hash, request.Start+i)
		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, body)
	}

	return &remote.EngineGetPayloadBodiesV1Response{Bodies: bodies}, nil
}

func extractPayloadBodyFromBlock(block *types.Block) (*types2.ExecutionPayloadBodyV1, error) {
	if block == nil {
		return nil, nil
	}

	txs := block.Transactions()
	bdTxs := make([][]byte, len(txs))
	for idx, tx := range txs {
		var buf bytes.Buffer
		if err := tx.MarshalBinary(&buf); err != nil {
			return nil, err
		} else {
			bdTxs[idx] = buf.Bytes()
		}
	}

	wds := block.Withdrawals()
	bdWds := make([]*types2.Withdrawal, len(wds))

	if wds == nil {
		// pre shanghai blocks could have nil withdrawals so nil the slice as per spec
		bdWds = nil
	} else {
		for idx, wd := range wds {
			bdWds[idx] = &types2.Withdrawal{
				Index:          wd.Index,
				ValidatorIndex: wd.Validator,
				Address:        gointerfaces.ConvertAddressToH160(wd.Address),
				Amount:         wd.Amount,
			}
		}
	}

	return &types2.ExecutionPayloadBodyV1{Transactions: bdTxs, Withdrawals: bdWds}, nil
}

func (s *EthBackendServer) evictOldBuilders() {
	ids := common.SortedKeys(s.builders)

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

func ConvertWithdrawalsFromRpc(in []*types2.Withdrawal) []*types.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*types.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &types.Withdrawal{
			Index:     w.Index,
			Validator: w.ValidatorIndex,
			Address:   gointerfaces.ConvertH160toAddress(w.Address),
			Amount:    w.Amount,
		})
	}
	return out
}

func ConvertWithdrawalsToRpc(in []*types.Withdrawal) []*types2.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*types2.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &types2.Withdrawal{
			Index:          w.Index,
			ValidatorIndex: w.Validator,
			Address:        gointerfaces.ConvertAddressToH160(w.Address),
			Amount:         w.Amount,
		})
	}
	return out
}
