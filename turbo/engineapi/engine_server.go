package engineapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"google.golang.org/protobuf/types/known/emptypb"
)

type EngineServer struct {
	hd     *headerdownload.HeaderDownload
	config *chain.Config
	// Block proposing for proof-of-stake
	payloadId      uint64
	lastParameters *core.BlockBuilderParameters
	builders       map[uint64]*builder.BlockBuilder
	builderFunc    builder.BlockBuilderFunc
	proposing      bool

	ctx    context.Context
	lock   sync.Mutex
	logger log.Logger

	db          kv.RoDB
	blockReader services.FullBlockReader

	engine.UnimplementedEngineServer
}

func NewEngineServer(ctx context.Context, logger log.Logger, config *chain.Config, builderFunc builder.BlockBuilderFunc,
	db kv.RoDB, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, proposing bool) *EngineServer {
	return &EngineServer{
		ctx:         ctx,
		logger:      logger,
		config:      config,
		builderFunc: builderFunc,
		db:          db,
		blockReader: blockReader,
		proposing:   proposing,
		hd:          hd,
		builders:    make(map[uint64]*builder.BlockBuilder),
	}
}

func (s *EngineServer) PendingBlock(_ context.Context, _ *emptypb.Empty) (*engine.PendingBlockReply, error) {
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

	return &engine.PendingBlockReply{BlockRlp: blockRlp}, nil
}

func convertPayloadStatus(payloadStatus *engine_helpers.PayloadStatus) *engine.EnginePayloadStatus {
	reply := engine.EnginePayloadStatus{Status: payloadStatus.Status}
	if payloadStatus.Status != engine.EngineStatus_SYNCING {
		reply.LatestValidHash = gointerfaces.ConvertHashToH256(payloadStatus.LatestValidHash)
	}
	if payloadStatus.ValidationError != nil {
		reply.ValidationError = payloadStatus.ValidationError.Error()
	}
	return &reply
}

func (s *EngineServer) stageLoopIsBusy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	wait, ok := s.hd.BeaconRequestList.WaitForWaiting(ctx)
	if !ok {
		select {
		case <-wait:
			return false
		case <-ctx.Done():
			return true
		}
	}
	return false
}

func (s *EngineServer) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !s.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if s.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

// EngineNewPayload validates and possibly executes payload
func (s *EngineServer) EngineNewPayload(ctx context.Context, req *types2.ExecutionPayload) (*engine.EnginePayloadStatus, error) {
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
		Difficulty:  merge.ProofOfStakeDifficulty,
		Nonce:       merge.ProofOfStakeNonce,
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

	if req.Version >= 3 {
		header.DataGasUsed = req.DataGasUsed
		header.ExcessDataGas = req.ExcessDataGas
	}

	if !s.config.IsCancun(header.Time) && (header.DataGasUsed != nil || header.ExcessDataGas != nil) {
		return nil, &rpc.InvalidParamsError{Message: "dataGasUsed/excessDataGas present before Cancun"}
	}

	if s.config.IsCancun(header.Time) && (header.DataGasUsed == nil || header.ExcessDataGas == nil) {
		return nil, &rpc.InvalidParamsError{Message: "dataGasUsed/excessDataGas missing"}
	}

	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
	if header.Hash() != blockHash {
		s.logger.Error("[NewPayload] invalid block hash", "stated", libcommon.Hash(blockHash), "actual", header.Hash())
		return &engine.EnginePayloadStatus{
			Status:          engine.EngineStatus_INVALID,
			ValidationError: "invalid block hash",
		}, nil
	}

	for _, txn := range req.Transactions {
		if types.TypedTransactionMarshalledAsRlpString(txn) {
			s.logger.Warn("[NewPayload] typed txn marshalled as RLP string", "txn", common.Bytes2Hex(txn))
			return &engine.EnginePayloadStatus{
				Status:          engine.EngineStatus_INVALID,
				ValidationError: "typed txn marshalled as RLP string",
			}, nil
		}
	}

	transactions, err := types.DecodeTransactions(req.Transactions)
	if err != nil {
		s.logger.Warn("[NewPayload] failed to decode transactions", "err", err)
		return &engine.EnginePayloadStatus{
			Status:          engine.EngineStatus_INVALID,
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

	s.logger.Debug("[NewPayload] sending block", "height", header.Number, "hash", libcommon.Hash(blockHash))
	s.hd.BeaconRequestList.AddPayloadRequest(block)

	payloadStatus := <-s.hd.PayloadStatusCh
	s.logger.Debug("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return convertPayloadStatus(&payloadStatus), nil
}

// Check if we can quickly determine the status of a newPayload or forkchoiceUpdated.
func (s *EngineServer) getQuickPayloadStatusIfPossible(blockHash libcommon.Hash, blockNumber uint64, parentHash libcommon.Hash, forkchoiceMessage *engine_helpers.ForkChoiceMessage, newPayload bool) (*engine_helpers.PayloadStatus, error) {
	// Determine which prefix to use for logs
	var prefix string
	if newPayload {
		prefix = "NewPayload"
	} else {
		prefix = "ForkChoiceUpdated"
	}
	if s.config.TerminalTotalDifficulty == nil {
		s.logger.Error(fmt.Sprintf("[%s] not a proof-of-stake chain", prefix))
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
		return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_VALID, LatestValidHash: blockHash}, nil
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
		s.logger.Warn(fmt.Sprintf("[%s] Beacon Chain request before TTD", prefix), "hash", blockHash)
		return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_INVALID, LatestValidHash: libcommon.Hash{}}, nil
	}

	if !s.hd.POSSync() {
		s.logger.Info(fmt.Sprintf("[%s] Still in PoW sync", prefix), "hash", blockHash)
		return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_SYNCING}, nil
	}

	var canonicalHash libcommon.Hash
	if header != nil {
		canonicalHash, err = s.blockReader.CanonicalHash(context.Background(), tx, header.Number.Uint64())
	}
	if err != nil {
		return nil, err
	}

	if newPayload && parent != nil && blockNumber != parent.Number.Uint64()+1 {
		s.logger.Warn(fmt.Sprintf("[%s] Invalid block number", prefix), "headerNumber", blockNumber, "parentNumber", parent.Number.Uint64())
		s.hd.ReportBadHeaderPoS(blockHash, parent.Hash())
		return &engine_helpers.PayloadStatus{
			Status:          engine.EngineStatus_INVALID,
			LatestValidHash: parent.Hash(),
			ValidationError: errors.New("invalid block number"),
		}, nil
	}
	// Check if we already determined if the hash is attributed to a previously received invalid header.
	bad, lastValidHash := s.hd.IsBadHeaderPoS(blockHash)
	if bad {
		s.logger.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash)
	} else if newPayload {
		bad, lastValidHash = s.hd.IsBadHeaderPoS(parentHash)
		if bad {
			s.logger.Warn(fmt.Sprintf("[%s] Previously known bad block", prefix), "hash", blockHash, "parentHash", parentHash)
		}
	}
	if bad {
		s.hd.ReportBadHeaderPoS(blockHash, lastValidHash)
		return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_INVALID, LatestValidHash: lastValidHash}, nil
	}

	// If header is already validated or has a missing parent, you can either return VALID or SYNCING.
	if newPayload {
		if header != nil && canonicalHash == blockHash {
			return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_VALID, LatestValidHash: blockHash}, nil
		}

		if parent == nil && s.hd.PosStatus() != headerdownload.Idle {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS blocks", prefix), "hash", blockHash)
			return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_SYNCING}, nil
		}
	} else {
		if header == nil && s.hd.PosStatus() != headerdownload.Idle {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS stuff", prefix), "hash", blockHash)
			return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_SYNCING}, nil
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
			return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_VALID, LatestValidHash: blockHash}, nil
		}
	}

	// If another payload is already commissioned then we just reply with syncing
	if s.stageLoopIsBusy() {
		s.logger.Debug(fmt.Sprintf("[%s] stage loop is busy", prefix))
		return &engine_helpers.PayloadStatus{Status: engine.EngineStatus_SYNCING}, nil
	}

	return nil, nil
}

// The expected value to be received by the feeRecipient in wei
func blockValue(br *types.BlockWithReceipts, baseFee *uint256.Int) *uint256.Int {
	blockValue := uint256.NewInt(0)
	txs := br.Block.Transactions()
	for i := range txs {
		gas := new(uint256.Int).SetUint64(br.Receipts[i].GasUsed)
		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)
		txValue := new(uint256.Int).Mul(gas, effectiveTip)
		blockValue.Add(blockValue, txValue)
	}
	return blockValue
}

// EngineGetPayload retrieves previously assembled payload (Validators only)
func (s *EngineServer) EngineGetPayload(ctx context.Context, req *engine.EngineGetPayloadRequest) (*engine.EngineGetPayloadResponse, error) {
	if !s.proposing {
		return nil, fmt.Errorf("execution layer not running as a proposer. enable proposer by taking out the --proposer.disable flag on startup")
	}

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	s.logger.Debug("[GetPayload] acquiring lock")
	s.lock.Lock()
	defer s.lock.Unlock()
	s.logger.Debug("[GetPayload] lock acquired")

	builder, ok := s.builders[req.PayloadId]
	if !ok {
		log.Warn("Payload not stored", "payloadId", req.PayloadId)
		return nil, &engine_helpers.UnknownPayloadErr
	}

	blockWithReceipts, err := builder.Stop()
	if err != nil {
		s.logger.Error("Failed to build PoS block", "err", err)
		return nil, err
	}
	block := blockWithReceipts.Block
	header := block.Header()

	baseFee := new(uint256.Int)
	baseFee.SetFromBig(header.BaseFee)

	encodedTransactions, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, err
	}

	payload := &types2.ExecutionPayload{
		Version:       1,
		ParentHash:    gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:      gointerfaces.ConvertAddressToH160(header.Coinbase),
		Timestamp:     header.Time,
		PrevRandao:    gointerfaces.ConvertHashToH256(header.MixDigest),
		StateRoot:     gointerfaces.ConvertHashToH256(block.Root()),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(block.ReceiptHash()),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(block.Bloom().Bytes()),
		GasLimit:      block.GasLimit(),
		GasUsed:       block.GasUsed(),
		BlockNumber:   block.NumberU64(),
		ExtraData:     block.Extra(),
		BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(baseFee),
		BlockHash:     gointerfaces.ConvertHashToH256(block.Hash()),
		Transactions:  encodedTransactions,
	}
	if block.Withdrawals() != nil {
		payload.Version = 2
		payload.Withdrawals = ConvertWithdrawalsToRpc(block.Withdrawals())
	}

	if header.DataGasUsed != nil && header.ExcessDataGas != nil {
		payload.Version = 3
		payload.DataGasUsed = header.DataGasUsed
		payload.ExcessDataGas = header.ExcessDataGas
	}

	blockValue := blockValue(blockWithReceipts, baseFee)

	blobsBundle := &types2.BlobsBundleV1{}
	for i, tx := range block.Transactions() {
		if tx.Type() != types.BlobTxType {
			continue
		}
		blobTx, ok := tx.(*types.BlobTxWrapper)
		if !ok {
			return nil, fmt.Errorf("expected blob transaction to be type BlobTxWrapper, got: %T", blobTx)
		}
		versionedHashes, commitments, proofs, blobs := blobTx.GetDataHashes(), blobTx.Commitments, blobTx.Proofs, blobTx.Blobs
		lenCheck := len(versionedHashes)
		if lenCheck != len(commitments) || lenCheck != len(proofs) || lenCheck != len(blobs) {
			return nil, fmt.Errorf("tx %d in block %s has inconsistent commitments (%d) / proofs (%d) / blobs (%d) / "+
				"versioned hashes (%d)", i, block.Hash(), len(commitments), len(proofs), len(blobs), lenCheck)
		}
		for _, commitment := range commitments {
			blobsBundle.Commitments = append(blobsBundle.Commitments, commitment[:])
		}
		for _, proof := range proofs {
			blobsBundle.Proofs = append(blobsBundle.Proofs, proof[:])
		}
		for _, blob := range blobs {
			blobsBundle.Blobs = append(blobsBundle.Blobs, blob[:])
		}
	}

	return &engine.EngineGetPayloadResponse{
		ExecutionPayload: payload,
		BlockValue:       gointerfaces.ConvertUint256IntToH256(blockValue),
		BlobsBundle:      blobsBundle,
	}, nil
}

// engineForkChoiceUpdated either states new block head or request the assembling of a new block
func (s *EngineServer) EngineForkChoiceUpdated(ctx context.Context, req *engine.EngineForkChoiceUpdatedRequest,
) (*engine.EngineForkChoiceUpdatedResponse, error) {
	forkChoice := engine_helpers.ForkChoiceMessage{
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
		s.logger.Debug("[ForkChoiceUpdated] sending forkChoiceMessage", "head", forkChoice.HeadBlockHash)
		s.hd.BeaconRequestList.AddForkChoiceRequest(&forkChoice)

		statusDeref := <-s.hd.PayloadStatusCh
		status = &statusDeref
		s.logger.Debug("[ForkChoiceUpdated] got reply", "payloadStatus", status)

		if status.CriticalError != nil {
			return nil, status.CriticalError
		}
	}

	// No need for payload building
	payloadAttributes := req.PayloadAttributes
	if payloadAttributes == nil || status.Status != engine.EngineStatus_VALID {
		return &engine.EngineForkChoiceUpdatedResponse{PayloadStatus: convertPayloadStatus(status)}, nil
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

		s.logger.Warn("Skipping payload building because forkchoiceState.headBlockHash is not the head of the canonical chain",
			"forkChoice.HeadBlockHash", forkChoice.HeadBlockHash, "headHeader.Hash", headHeader.Hash())
		return &engine.EngineForkChoiceUpdatedResponse{PayloadStatus: convertPayloadStatus(status)}, nil
	}

	if headHeader.Time >= payloadAttributes.Timestamp {
		return nil, &engine_helpers.InvalidPayloadAttributesErr
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

	// First check if we're already building a block with the requested parameters
	if reflect.DeepEqual(s.lastParameters, &param) {
		s.logger.Info("[ForkChoiceUpdated] duplicate build request")
		return &engine.EngineForkChoiceUpdatedResponse{
			PayloadStatus: &engine.EnginePayloadStatus{
				Status:          engine.EngineStatus_VALID,
				LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
			},
			PayloadId: s.payloadId,
		}, nil
	}

	// Initiate payload building
	s.evictOldBuilders()

	s.payloadId++
	param.PayloadId = s.payloadId
	s.lastParameters = &param

	s.builders[s.payloadId] = builder.NewBlockBuilder(s.builderFunc, &param)
	s.logger.Info("[ForkChoiceUpdated] BlockBuilder added", "payload", s.payloadId)

	return &engine.EngineForkChoiceUpdatedResponse{
		PayloadStatus: &engine.EnginePayloadStatus{
			Status:          engine.EngineStatus_VALID,
			LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		},
		PayloadId: s.payloadId,
	}, nil
}

func (s *EngineServer) EngineGetPayloadBodiesByHashV1(ctx context.Context, request *engine.EngineGetPayloadBodiesByHashV1Request) (*engine.EngineGetPayloadBodiesV1Response, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bodies := make([]*types2.ExecutionPayloadBodyV1, len(request.Hashes))

	for hashIdx, hash := range request.Hashes {
		h := gointerfaces.ConvertH256ToHash(hash)
		block, err := s.blockReader.BlockByHash(ctx, tx, h)
		if err != nil {
			return nil, err
		}
		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies[hashIdx] = body
	}

	return &engine.EngineGetPayloadBodiesV1Response{Bodies: bodies}, nil
}

func (s *EngineServer) EngineGetPayloadBodiesByRangeV1(ctx context.Context, request *engine.EngineGetPayloadBodiesByRangeV1Request) (*engine.EngineGetPayloadBodiesV1Response, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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

		block, _, err := s.blockReader.BlockWithSenders(ctx, tx, hash, request.Start+i)
		if err != nil {
			return nil, err
		}
		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, body)
	}

	return &engine.EngineGetPayloadBodiesV1Response{Bodies: bodies}, nil
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

func (s *EngineServer) evictOldBuilders() {
	ids := common.SortedKeys(s.builders)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(s.builders)-engine_helpers.MaxBuilders; i++ {
		delete(s.builders, ids[i])
	}
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
