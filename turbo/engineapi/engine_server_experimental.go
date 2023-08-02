package engineapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_block_downloader"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

type EngineServerExperimental struct {
	hd              *headerdownload.HeaderDownload
	blockDownloader *engine_block_downloader.EngineBlockDownloader
	config          *chain.Config
	// Block proposing for proof-of-stake
	proposing        bool
	test             bool
	executionService execution.ExecutionClient

	ctx    context.Context
	lock   sync.Mutex
	logger log.Logger

	db          kv.RoDB
	blockReader services.FullBlockReader
}

func NewEngineServerExperimental(ctx context.Context, logger log.Logger, config *chain.Config, executionService execution.ExecutionClient,
	db kv.RoDB, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload,
	blockDownloader *engine_block_downloader.EngineBlockDownloader, test bool, proposing bool) *EngineServerExperimental {
	return &EngineServerExperimental{
		ctx:              ctx,
		logger:           logger,
		config:           config,
		executionService: executionService,
		blockDownloader:  blockDownloader,
		db:               db,
		blockReader:      blockReader,
		proposing:        proposing,
		hd:               hd,
	}
}

func (e *EngineServerExperimental) Start(httpConfig httpcfg.HttpCfg,
	filters *rpchelper.Filters, stateCache kvcache.Cache, agg *libstate.AggregatorV3, engineReader consensus.EngineReader,
	eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient) {
	base := jsonrpc.NewBaseApi(filters, stateCache, e.blockReader, agg, httpConfig.WithDatadir, httpConfig.EvmCallTimeout, engineReader, httpConfig.Dirs)

	ethImpl := jsonrpc.NewEthAPI(base, e.db, eth, txPool, mining, httpConfig.Gascap, httpConfig.ReturnDataLimit, e.logger)

	// engineImpl := NewEngineAPI(base, db, engineBackend)
	// e.startEngineMessageHandler()
	apiList := []rpc.API{
		{
			Namespace: "eth",
			Public:    true,
			Service:   jsonrpc.EthAPI(ethImpl),
			Version:   "1.0",
		}, {
			Namespace: "engine",
			Public:    true,
			Service:   EngineAPI(e),
			Version:   "1.0",
		}}

	if err := cli.StartRpcServerWithJwtAuthentication(e.ctx, httpConfig, apiList, e.logger); err != nil {
		e.logger.Error(err.Error())
	}
}

func (s *EngineServerExperimental) stageLoopIsBusy() bool {
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

func (s *EngineServerExperimental) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !s.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if s.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

// EngineNewPayload validates and possibly executes payload
func (s *EngineServerExperimental) newPayload(ctx context.Context, req *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, version clparams.StateVersion,
) (*engine_types.PayloadStatus, error) {
	var bloom types.Bloom
	copy(bloom[:], req.LogsBloom)

	txs := [][]byte{}
	for _, transaction := range req.Transactions {
		txs = append(txs, transaction)
	}

	header := types.Header{
		ParentHash:  req.ParentHash,
		Coinbase:    req.FeeRecipient,
		Root:        req.StateRoot,
		Bloom:       bloom,
		BaseFee:     (*big.Int)(req.BaseFeePerGas),
		Extra:       req.ExtraData,
		Number:      big.NewInt(int64(req.BlockNumber)),
		GasUsed:     uint64(req.GasUsed),
		GasLimit:    uint64(req.GasLimit),
		Time:        uint64(req.Timestamp),
		MixDigest:   req.PrevRandao,
		UncleHash:   types.EmptyUncleHash,
		Difficulty:  merge.ProofOfStakeDifficulty,
		Nonce:       merge.ProofOfStakeNonce,
		ReceiptHash: req.ReceiptsRoot,
		TxHash:      types.DeriveSha(types.BinaryTransactions(txs)),
	}
	var withdrawals []*types.Withdrawal
	if version >= clparams.CapellaVersion {
		withdrawals = req.Withdrawals
	}

	if withdrawals != nil {
		wh := types.DeriveSha(types.Withdrawals(withdrawals))
		header.WithdrawalsHash = &wh
	}

	if err := s.checkWithdrawalsPresence(header.Time, withdrawals); err != nil {
		return nil, err
	}

	if version >= clparams.DenebVersion {
		header.BlobGasUsed = (*uint64)(req.BlobGasUsed)
		header.ExcessBlobGas = (*uint64)(req.ExcessBlobGas)
	}

	if !s.config.IsCancun(header.Time) && (header.BlobGasUsed != nil || header.ExcessBlobGas != nil) {
		return nil, &rpc.InvalidParamsError{Message: "blobGasUsed/excessBlobGas present before Cancun"}
	}

	if s.config.IsCancun(header.Time) && (header.BlobGasUsed == nil || header.ExcessBlobGas == nil) {
		return nil, &rpc.InvalidParamsError{Message: "blobGasUsed/excessBlobGas missing"}
	}

	blockHash := req.BlockHash
	if header.Hash() != blockHash {
		s.logger.Error("[NewPayload] invalid block hash", "stated", blockHash, "actual", header.Hash())
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			ValidationError: engine_types.NewStringifiedErrorFromString("invalid block hash"),
		}, nil
	}

	for _, txn := range req.Transactions {
		if types.TypedTransactionMarshalledAsRlpString(txn) {
			s.logger.Warn("[NewPayload] typed txn marshalled as RLP string", "txn", common.Bytes2Hex(txn))
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString("typed txn marshalled as RLP string"),
			}, nil
		}
	}

	transactions, err := types.DecodeTransactions(txs)
	if err != nil {
		s.logger.Warn("[NewPayload] failed to decode transactions", "err", err)
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			ValidationError: engine_types.NewStringifiedError(err),
		}, nil
	}
	if version >= clparams.DenebVersion {
		actualBlobHashes := []libcommon.Hash{}
		for _, tx := range transactions {
			actualBlobHashes = append(actualBlobHashes, tx.GetBlobHashes()...)
		}
		if !reflect.DeepEqual(actualBlobHashes, expectedBlobHashes) {
			s.logger.Warn("[NewPayload] mismatch in blob hashes",
				"expectedBlobHashes", expectedBlobHashes, "actualBlobHashes", actualBlobHashes)
			return &engine_types.PayloadStatus{
				Status:          engine_types.InvalidStatus,
				ValidationError: engine_types.NewStringifiedErrorFromString("mismatch in blob hashes"),
			}, nil
		}
	}

	possibleStatus, err := s.getQuickPayloadStatusIfPossible(blockHash, uint64(req.BlockNumber), header.ParentHash, nil, true)
	if err != nil {
		return nil, err
	}
	if possibleStatus != nil {
		return possibleStatus, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Debug("[NewPayload] sending block", "height", header.Number, "hash", blockHash)
	block := types.NewBlockFromStorage(blockHash, &header, transactions, nil /* uncles */, withdrawals)
	s.hd.BeaconRequestList.AddPayloadRequest(block)

	chainReader := eth1_chain_reader.NewChainReaderEth1(s.ctx, s.config, s.executionService)
	payloadStatus, err := s.handleNewPayload("NewPayload", block, chainReader)
	if err != nil {
		return nil, err
	}
	s.logger.Debug("[NewPayload] got reply", "payloadStatus", payloadStatus)

	if payloadStatus.CriticalError != nil {
		return nil, payloadStatus.CriticalError
	}

	return payloadStatus, nil
}

// Check if we can quickly determine the status of a newPayload or forkchoiceUpdated.
func (s *EngineServerExperimental) getQuickPayloadStatusIfPossible(blockHash libcommon.Hash, blockNumber uint64, parentHash libcommon.Hash, forkchoiceMessage *engine_types.ForkChoiceState, newPayload bool) (*engine_types.PayloadStatus, error) {
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
		forkchoiceMessage.HeadHash == rawdb.ReadForkchoiceHead(tx) &&
		forkchoiceMessage.SafeBlockHash == rawdb.ReadForkchoiceSafe(tx) {
		return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
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
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &libcommon.Hash{}}, nil
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
		parentHash := parent.Hash()
		return &engine_types.PayloadStatus{
			Status:          engine_types.InvalidStatus,
			LatestValidHash: &parentHash,
			ValidationError: engine_types.NewStringifiedErrorFromString("invalid block number"),
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
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus, LatestValidHash: &lastValidHash}, nil
	}

	// If header is already validated or has a missing parent, you can either return VALID or SYNCING.
	if newPayload {
		if header != nil && canonicalHash == blockHash {
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
		}

		if parent == nil && s.hd.PosStatus() == headerdownload.Syncing {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS blocks", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	} else {
		if header == nil && s.hd.PosStatus() == headerdownload.Syncing {
			s.logger.Debug(fmt.Sprintf("[%s] Downloading some other PoS stuff", prefix), "hash", blockHash)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
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
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &blockHash}, nil
		}
	}

	return nil, nil
}

// EngineGetPayload retrieves previously assembled payload (Validators only)
func (s *EngineServerExperimental) getPayload(ctx context.Context, payloadId uint64) (*engine_types.GetPayloadResponse, error) {
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
	resp, err := s.executionService.GetAssembledBlock(ctx, &execution.GetAssembledBlockRequest{
		Id: payloadId,
	})
	if err != nil {
		return nil, err
	}
	if resp.Busy {
		log.Warn("Cannot build payload, execution is busy", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr
	}
	// If the service is busy or there is no data for the given id then respond accordingly.
	if resp.Data == nil {
		log.Warn("Payload not stored", "payloadId", payloadId)
		return nil, &engine_helpers.UnknownPayloadErr

	}
	data := resp.Data

	return &engine_types.GetPayloadResponse{
		ExecutionPayload: engine_types.ConvertPayloadFromRpc(data.ExecutionPayload),
		BlockValue:       (*hexutil.Big)(gointerfaces.ConvertH256ToUint256Int(data.BlockValue).ToBig()),
		BlobsBundle:      engine_types.ConvertBlobsFromRpc(data.BlobsBundle),
	}, nil
}

// engineForkChoiceUpdated either states new block head or request the assembling of a new block
func (s *EngineServerExperimental) forkchoiceUpdated(ctx context.Context, forkchoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes, version clparams.StateVersion,
) (*engine_types.ForkChoiceUpdatedResponse, error) {
	status, err := s.getQuickPayloadStatusIfPossible(forkchoiceState.HeadHash, 0, libcommon.Hash{}, forkchoiceState, false)
	if err != nil {
		return nil, err
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	chainReader := eth1_chain_reader.NewChainReaderEth1(s.ctx, s.config, s.executionService)

	if status == nil {
		s.logger.Debug("[ForkChoiceUpdated] sending forkChoiceMessage", "head", forkchoiceState.HeadHash)

		status, err = s.handlesForkChoice("ForkChoiceUpdated", chainReader, forkchoiceState, 0)
		if err != nil {
			return nil, err
		}
		s.logger.Debug("[ForkChoiceUpdated] got reply", "payloadStatus", status)

		if status.CriticalError != nil {
			return nil, status.CriticalError
		}
	}

	// No need for payload building
	if payloadAttributes == nil || status.Status != engine_types.ValidStatus {
		return &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: status}, nil
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

	if headHeader.Hash() != forkchoiceState.HeadHash {
		// Per Item 2 of https://github.com/ethereum/execution-apis/blob/v1.0.0-alpha.9/src/engine/specification.md#specification-1:
		// Client software MAY skip an update of the forkchoice state and
		// MUST NOT begin a payload build process if forkchoiceState.headBlockHash doesn't reference a leaf of the block tree.
		// That is, the block referenced by forkchoiceState.headBlockHash is neither the head of the canonical chain nor a block at the tip of any other chain.
		// In the case of such an event, client software MUST return
		// {payloadStatus: {status: VALID, latestValidHash: forkchoiceState.headBlockHash, validationError: null}, payloadId: null}.

		s.logger.Warn("Skipping payload building because forkchoiceState.headBlockHash is not the head of the canonical chain",
			"forkChoice.HeadBlockHash", forkchoiceState.HeadHash, "headHeader.Hash", headHeader.Hash())
		return &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: status}, nil
	}

	if headHeader.Time >= uint64(payloadAttributes.Timestamp) {
		return nil, &engine_helpers.InvalidPayloadAttributesErr
	}

	req := &execution.AssembleBlockRequest{
		ParentHash:           gointerfaces.ConvertHashToH256(forkchoiceState.HeadHash),
		Timestamp:            uint64(payloadAttributes.Timestamp),
		MixDigest:            gointerfaces.ConvertHashToH256(payloadAttributes.PrevRandao),
		SuggestedFeeRecipent: gointerfaces.ConvertAddressToH160(payloadAttributes.SuggestedFeeRecipient),
	}

	if version >= clparams.CapellaVersion {
		req.Withdrawals = engine_types.ConvertWithdrawalsToRpc(payloadAttributes.Withdrawals)
	}

	resp, err := s.executionService.AssembleBlock(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Busy {
		return nil, errors.New("[ForkChoiceUpdated]: execution service is busy, cannot assemble blocks")
	}
	return &engine_types.ForkChoiceUpdatedResponse{
		PayloadStatus: &engine_types.PayloadStatus{
			Status:          engine_types.ValidStatus,
			LatestValidHash: &headHash,
		},
		PayloadId: engine_types.ConvertPayloadId(resp.Id),
	}, nil
}

func (s *EngineServerExperimental) getPayloadBodiesByHash(ctx context.Context, request []libcommon.Hash, _ clparams.StateVersion) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bodies := make([]*engine_types.ExecutionPayloadBodyV1, len(request))

	for hashIdx, hash := range request {
		block, err := s.blockReader.BlockByHash(ctx, tx, hash)
		if err != nil {
			return nil, err
		}
		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies[hashIdx] = body
	}

	return bodies, nil
}

func extractPayloadBodyFromBlock(block *types.Block) (*engine_types.ExecutionPayloadBodyV1, error) {
	if block == nil {
		return nil, nil
	}

	txs := block.Transactions()
	bdTxs := make([]hexutility.Bytes, len(txs))
	for idx, tx := range txs {
		var buf bytes.Buffer
		if err := tx.MarshalBinary(&buf); err != nil {
			return nil, err
		} else {
			bdTxs[idx] = buf.Bytes()
		}
	}

	return &engine_types.ExecutionPayloadBodyV1{Transactions: bdTxs, Withdrawals: block.Withdrawals()}, nil
}

func (s *EngineServerExperimental) getPayloadBodiesByRange(ctx context.Context, start, count uint64, _ clparams.StateVersion) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bodies := make([]*engine_types.ExecutionPayloadBodyV1, 0, count)

	for i := uint64(0); i < count; i++ {
		hash, err := rawdb.ReadCanonicalHash(tx, start+i)
		if err != nil {
			return nil, err
		}
		if hash == (libcommon.Hash{}) {
			// break early if beyond the last known canonical header
			break
		}

		block, _, err := s.blockReader.BlockWithSenders(ctx, tx, hash, start+i)
		if err != nil {
			return nil, err
		}
		body, err := extractPayloadBodyFromBlock(block)
		if err != nil {
			return nil, err
		}
		bodies = append(bodies, body)
	}

	return bodies, nil
}

func (e *EngineServerExperimental) GetPayloadV1(ctx context.Context, payloadId hexutility.Bytes) (*engine_types.ExecutionPayload, error) {

	decodedPayloadId := binary.BigEndian.Uint64(payloadId)
	log.Info("Received GetPayloadV1", "payloadId", decodedPayloadId)

	response, err := e.getPayload(ctx, decodedPayloadId)
	if err != nil {
		return nil, err
	}

	return response.ExecutionPayload, nil
}

func (e *EngineServerExperimental) GetPayloadV2(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	log.Info("Received GetPayloadV2", "payloadId", decodedPayloadId)

	return e.getPayload(ctx, decodedPayloadId)
}

func (e *EngineServerExperimental) GetPayloadV3(ctx context.Context, payloadID hexutility.Bytes) (*engine_types.GetPayloadResponse, error) {
	decodedPayloadId := binary.BigEndian.Uint64(payloadID)
	log.Info("Received GetPayloadV3", "payloadId", decodedPayloadId)

	return e.getPayload(ctx, decodedPayloadId)
}

func (e *EngineServerExperimental) ForkchoiceUpdatedV1(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.BellatrixVersion)
}

func (e *EngineServerExperimental) ForkchoiceUpdatedV2(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.CapellaVersion)
}

func (e *EngineServerExperimental) ForkchoiceUpdatedV3(ctx context.Context, forkChoiceState *engine_types.ForkChoiceState, payloadAttributes *engine_types.PayloadAttributes) (*engine_types.ForkChoiceUpdatedResponse, error) {
	return e.forkchoiceUpdated(ctx, forkChoiceState, payloadAttributes, clparams.CapellaVersion)
}

// NewPayloadV1 processes new payloads (blocks) from the beacon chain without withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_newpayloadv1
func (e *EngineServerExperimental) NewPayloadV1(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, clparams.BellatrixVersion)
}

// NewPayloadV2 processes new payloads (blocks) from the beacon chain with withdrawals.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_newpayloadv2
func (e *EngineServerExperimental) NewPayloadV2(ctx context.Context, payload *engine_types.ExecutionPayload) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, nil, clparams.CapellaVersion)
}

// NewPayloadV3 processes new payloads (blocks) from the beacon chain with withdrawals & blob gas.
// See https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#engine_newpayloadv3
func (e *EngineServerExperimental) NewPayloadV3(ctx context.Context, payload *engine_types.ExecutionPayload,
	expectedBlobHashes []libcommon.Hash, parentBeaconBlockRoot *libcommon.Hash) (*engine_types.PayloadStatus, error) {
	return e.newPayload(ctx, payload, expectedBlobHashes, clparams.DenebVersion)
}

// Receives consensus layer's transition configuration and checks if the execution layer has the correct configuration.
// Can also be used to ping the execution layer (heartbeats).
// See https://github.com/ethereum/execution-apis/blob/v1.0.0-beta.1/src/engine/specification.md#engine_exchangetransitionconfigurationv1
func (e *EngineServerExperimental) ExchangeTransitionConfigurationV1(ctx context.Context, beaconConfig *engine_types.TransitionConfiguration) (*engine_types.TransitionConfiguration, error) {
	terminalTotalDifficulty := e.config.TerminalTotalDifficulty

	if terminalTotalDifficulty == nil {
		return nil, fmt.Errorf("the execution layer doesn't have a terminal total difficulty. expected: %v", beaconConfig.TerminalTotalDifficulty)
	}

	if terminalTotalDifficulty.Cmp((*big.Int)(beaconConfig.TerminalTotalDifficulty)) != 0 {
		return nil, fmt.Errorf("the execution layer has a wrong terminal total difficulty. expected %v, but instead got: %d", beaconConfig.TerminalTotalDifficulty, terminalTotalDifficulty)
	}

	return &engine_types.TransitionConfiguration{
		TerminalTotalDifficulty: (*hexutil.Big)(terminalTotalDifficulty),
		TerminalBlockHash:       libcommon.Hash{},
		TerminalBlockNumber:     (*hexutil.Big)(libcommon.Big0),
	}, nil
}

func (e *EngineServerExperimental) GetPayloadBodiesByHashV1(ctx context.Context, hashes []libcommon.Hash) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	if len(hashes) > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}

	return e.getPayloadBodiesByHash(ctx, hashes, clparams.DenebVersion)
}

func (e *EngineServerExperimental) GetPayloadBodiesByRangeV1(ctx context.Context, start, count hexutil.Uint64) ([]*engine_types.ExecutionPayloadBodyV1, error) {
	if start == 0 || count == 0 {
		return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("invalid start or count, start: %v count: %v", start, count)}
	}
	if count > 1024 {
		return nil, &engine_helpers.TooLargeRequestErr
	}

	return e.getPayloadBodiesByRange(ctx, uint64(start), uint64(count), clparams.CapellaVersion)
}

var ourCapabilities = []string{
	"engine_forkchoiceUpdatedV1",
	"engine_forkchoiceUpdatedV2",
	"engine_newPayloadV1",
	"engine_newPayloadV2",
	// "engine_newPayloadV3",
	"engine_getPayloadV1",
	"engine_getPayloadV2",
	// "engine_getPayloadV3",
	"engine_exchangeTransitionConfigurationV1",
	"engine_getPayloadBodiesByHashV1",
	"engine_getPayloadBodiesByRangeV1",
}

func (e *EngineServerExperimental) ExchangeCapabilities(fromCl []string) []string {
	missingOurs := compareCapabilities(fromCl, ourCapabilities)
	missingCl := compareCapabilities(ourCapabilities, fromCl)

	if len(missingCl) > 0 || len(missingOurs) > 0 {
		log.Debug("ExchangeCapabilities mismatches", "cl_unsupported", missingCl, "erigon_unsupported", missingOurs)
	}

	return ourCapabilities
}

func compareCapabilities(from []string, to []string) []string {
	result := make([]string, 0)
	for _, f := range from {
		found := false
		for _, t := range to {
			if f == t {
				found = true
				break
			}
		}
		if !found {
			result = append(result, f)
		}
	}

	return result
}
