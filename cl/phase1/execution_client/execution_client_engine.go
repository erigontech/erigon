// Copyright 2026 The Erigon Authors
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

package execution_client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

var ErrNotSupported = errors.New("operation not supported in engine API mode")

const DefaultRPCHTTPTimeout = 30 * time.Second

func checkPayloadStatus(payloadStatus *engine_types.PayloadStatus) error {
	if payloadStatus == nil {
		return nil
	}
	validationErr := payloadStatus.ValidationError
	if validationErr != nil {
		return fmt.Errorf("engine payload status error: %w", validationErr.Error())
	}
	return nil
}

func checkForkChoiceStatus(payloadStatus *engine_types.PayloadStatus) error {
	if payloadStatus == nil {
		return nil
	}
	if payloadStatus.Status == engine_types.InvalidStatus || payloadStatus.Status == engine_types.InvalidBlockHashStatus {
		if payloadStatus.ValidationError != nil {
			return fmt.Errorf("%w: %w", ErrForkChoiceNotAdopted, payloadStatus.ValidationError.Error())
		}
		return fmt.Errorf("%w: status %s", ErrForkChoiceNotAdopted, payloadStatus.Status)
	}
	return checkPayloadStatus(payloadStatus)
}

// ExecutionClientEngine implements ExecutionEngine with either an in-process or
// remote EngineAPI. Local mode accesses chain data and the transaction pool
// directly; remote mode uses authenticated HTTP JSON-RPC for Engine API and
// eth_* calls.
type ExecutionClientEngine struct {
	engine    engineapi.EngineAPI
	chainRW   *chainreader.ChainReaderWriterEth1 // non-nil for local mode
	rpcClient *rpc.Client                        // non-nil for remote mode (eth_* calls)
	txpool    txpoolproto.TxpoolClient           // non-nil for local mode
	beaconCfg *clparams.BeaconChainConfig
}

// NewExecutionClientEngineLocal creates an in-process client with direct chain
// and transaction-pool access. The supplied EngineAPI is called without HTTP,
// JWT, or JSON serialization.
func NewExecutionClientEngineLocal(
	engine engineapi.EngineAPI,
	chainRW chainreader.ChainReaderWriterEth1,
	txpool txpoolproto.TxpoolClient,
	beaconCfg *clparams.BeaconChainConfig,
) (*ExecutionClientEngine, error) {
	cc := &ExecutionClientEngine{
		engine:  engine,
		chainRW: &chainRW,
		txpool:  txpool,
	}
	cc.SetBeaconChainConfig(beaconCfg)
	return cc, nil
}

// NewExecutionClientEngineRPC creates a remote engine client that communicates
// over HTTP JSON-RPC with JWT authentication.
func NewExecutionClientEngineRPC(jwtSecret []byte, addr string, port int, beaconCfg *clparams.BeaconChainConfig) (*ExecutionClientEngine, error) {
	engineRPC, rpcClient, err := NewEngineAPIRPCClient(jwtSecret, addr, port)
	if err != nil {
		return nil, err
	}
	return &ExecutionClientEngine{
		engine:    engineRPC,
		rpcClient: rpcClient,
		beaconCfg: beaconCfg,
	}, nil
}

func (cc *ExecutionClientEngine) isLocal() bool {
	return cc.chainRW != nil
}

func (cc *ExecutionClientEngine) SetBeaconChainConfig(beaconCfg *clparams.BeaconChainConfig) {
	cc.beaconCfg = beaconCfg
	if engineWithCfg, ok := cc.engine.(interface {
		SetBeaconChainConfig(*clparams.BeaconChainConfig)
	}); ok {
		engineWithCfg.SetBeaconChainConfig(beaconCfg)
	}
}

func (cc *ExecutionClientEngine) Close() {
	if cc.rpcClient != nil {
		cc.rpcClient.Close()
	}
}

func (cc *ExecutionClientEngine) NewPayload(
	ctx context.Context,
	payload *cltypes.Eth1Block,
	beaconParentRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (PayloadStatus, error) {
	if payload == nil {
		return PayloadStatusValidated, nil
	}

	request := engine_types.ExecutionPayloadFromSSZBlock(payload, payload.Version())

	var (
		payloadStatus *engine_types.PayloadStatus
		err           error
	)

	switch payload.Version() {
	case clparams.BellatrixVersion:
		payloadStatus, err = cc.engine.NewPayloadV1(ctx, request)
	case clparams.CapellaVersion:
		payloadStatus, err = cc.engine.NewPayloadV2(ctx, request)
	case clparams.DenebVersion:
		payloadStatus, err = cc.engine.NewPayloadV3(ctx, request, versionedHashes, beaconParentRoot)
	case clparams.ElectraVersion, clparams.FuluVersion:
		payloadStatus, err = cc.engine.NewPayloadV4(ctx, request, versionedHashes, beaconParentRoot, executionRequestsList)
	default: // Gloas+ (Amsterdam)
		payloadStatus, err = cc.engine.NewPayloadV5(ctx, request, versionedHashes, beaconParentRoot, executionRequestsList)
	}
	if err != nil {
		return PayloadStatusNone, fmt.Errorf("engine NewPayload failed: %w", err)
	}
	if payloadStatus == nil {
		return PayloadStatusNone, errors.New("engine NewPayload returned nil status")
	}

	if payloadStatus.Status == engine_types.AcceptedStatus {
		log.Info("[ExecutionClientEngine] New block accepted")
	}
	return newPayloadStatusByEngineStatus(payloadStatus.Status), checkPayloadStatus(payloadStatus)
}

func (cc *ExecutionClientEngine) ForkChoiceUpdate(
	ctx context.Context,
	finalized, safe, head common.Hash,
	attributes *engine_types.PayloadAttributes,
	version clparams.StateVersion,
) ([]byte, error) {
	forkChoiceState := &engine_types.ForkChoiceState{
		HeadHash:           head,
		SafeBlockHash:      safe,
		FinalizedBlockHash: finalized,
	}

	log.Debug("[ExecutionClientEngine] Calling ForkchoiceUpdated", "version", version)
	var (
		resp *engine_types.ForkChoiceUpdatedResponse
		err  error
	)
	switch version {
	case clparams.BellatrixVersion:
		resp, err = cc.engine.ForkchoiceUpdatedV1(ctx, forkChoiceState, attributes)
	case clparams.CapellaVersion:
		resp, err = cc.engine.ForkchoiceUpdatedV2(ctx, forkChoiceState, attributes)
	case clparams.DenebVersion, clparams.ElectraVersion, clparams.FuluVersion:
		// V3 is valid for Cancun (Deneb), Prague (Electra), and Osaka (Fulu).
		resp, err = cc.engine.ForkchoiceUpdatedV3(ctx, forkChoiceState, attributes)
	default: // Gloas+ (Amsterdam)
		resp, err = cc.engine.ForkchoiceUpdatedV4(ctx, forkChoiceState, attributes, nil)
	}
	if err != nil {
		if isDeadlineExceeded(err) {
			return nil, fmt.Errorf("%w: %w", ErrForkChoiceUpdateTimeout, err)
		}
		return nil, fmt.Errorf("engine ForkchoiceUpdated failed: %w", err)
	}

	if err := checkForkChoiceStatus(resp.PayloadStatus); err != nil {
		return nil, err
	}
	if attributes == nil && cc.isLocal() && resp.PayloadStatus != nil && resp.PayloadStatus.Status == engine_types.SyncingStatus {
		// A local SYNCING response for a known head is transient execution-module contention.
		knownHead, err := cc.chainRW.HasBlock(ctx, head)
		if err == nil && knownHead {
			return nil, ErrForkChoiceBusy
		}
	}
	if resp.PayloadId == nil {
		if attributes != nil {
			return nil, ErrForkChoiceUpdateNoPayloadID
		}
		return []byte{}, nil
	}
	return *resp.PayloadId, nil
}

func (cc *ExecutionClientEngine) SupportInsertion() bool {
	return cc.isLocal()
}

func (cc *ExecutionClientEngine) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	if !cc.isLocal() {
		return ErrNotSupported
	}
	return cc.chainRW.InsertBlocks(ctx, blocks)
}

func (cc *ExecutionClientEngine) InsertBlock(ctx context.Context, block *types.Block) error {
	if !cc.isLocal() {
		return ErrNotSupported
	}
	return cc.chainRW.InsertBlock(ctx, block)
}

func (cc *ExecutionClientEngine) CurrentHeader(ctx context.Context) (*types.Header, error) {
	if cc.isLocal() {
		return cc.chainRW.CurrentHeader(ctx), nil
	}
	var header *types.Header
	if err := cc.rpcClient.CallContext(ctx, &header, "eth_getBlockByNumber", "latest", false); err != nil {
		return nil, fmt.Errorf("eth_getBlockByNumber failed: %w", err)
	}
	return header, nil
}

func (cc *ExecutionClientEngine) IsCanonicalHash(ctx context.Context, hash common.Hash) (bool, error) {
	if cc.isLocal() {
		return cc.chainRW.IsCanonicalHash(ctx, hash)
	}
	var header *types.Header
	if err := cc.rpcClient.CallContext(ctx, &header, "eth_getBlockByHash", hash, false); err != nil {
		return false, fmt.Errorf("eth_getBlockByHash failed: %w", err)
	}
	if header == nil {
		return false, nil
	}
	// eth_getBlockByHash returns non-canonical blocks too — verify canonicality
	// by fetching the canonical block at this height and comparing hashes.
	var canonical *types.Header
	if err := cc.rpcClient.CallContext(ctx, &canonical, "eth_getBlockByNumber", hexutil.EncodeBig(header.Number.ToBig()), false); err != nil {
		return false, fmt.Errorf("eth_getBlockByNumber failed: %w", err)
	}
	return canonical != nil && canonical.Hash() == hash, nil
}

func (cc *ExecutionClientEngine) Ready(ctx context.Context) (bool, error) {
	if cc.isLocal() {
		return cc.chainRW.Ready(ctx)
	}
	return true, nil
}

func (cc *ExecutionClientEngine) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	if cc.isLocal() {
		return cc.chainRW.GetBodiesByRange(ctx, start, count)
	}
	result, err := cc.engine.GetPayloadBodiesByRangeV1(ctx, hexutil.Uint64(start), hexutil.Uint64(count))
	if err != nil {
		return nil, err
	}
	return payloadBodiesToRawBodies(result), nil
}

func (cc *ExecutionClientEngine) GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	if cc.isLocal() {
		return cc.chainRW.GetBodiesByHashes(ctx, hashes)
	}
	result, err := cc.engine.GetPayloadBodiesByHashV1(ctx, hashes)
	if err != nil {
		return nil, err
	}
	return payloadBodiesToRawBodies(result), nil
}

func payloadBodiesToRawBodies(bodies []*engine_types.ExecutionPayloadBody) []*types.RawBody {
	ret := make([]*types.RawBody, len(bodies))
	for i, body := range bodies {
		if body == nil {
			continue
		}
		ret[i] = &types.RawBody{
			Withdrawals: body.Withdrawals,
		}
		for _, txn := range body.Transactions {
			ret[i].Transactions = append(ret[i].Transactions, txn)
		}
	}
	return ret
}

func (cc *ExecutionClientEngine) FrozenBlocks(ctx context.Context) uint64 {
	if cc.isLocal() {
		frozenBlocks, _ := cc.chainRW.FrozenBlocks(ctx)
		return frozenBlocks
	}
	return 0
}

func (cc *ExecutionClientEngine) HasBlock(ctx context.Context, hash common.Hash) (bool, error) {
	if cc.isLocal() {
		return cc.chainRW.HasBlock(ctx, hash)
	}
	var header *types.Header
	if err := cc.rpcClient.CallContext(ctx, &header, "eth_getBlockByHash", hash, false); err != nil {
		return false, fmt.Errorf("eth_getBlockByHash failed: %w", err)
	}
	return header != nil, nil
}

func (cc *ExecutionClientEngine) GetAssembledBlock(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	if cc.isLocal() {
		return cc.chainRW.GetAssembledBlock(ctx, binary.LittleEndian.Uint64(id))
	}

	// GetPayload versions advance with the response fields introduced by each fork.
	switch {
	case version >= clparams.GloasVersion:
		return cc.getAssembledBlockV6(ctx, id, version)
	case version >= clparams.FuluVersion:
		return cc.getAssembledBlockV5(ctx, id, version)
	case version >= clparams.ElectraVersion:
		return cc.getAssembledBlockV4(ctx, id, version)
	default:
		return cc.getAssembledBlockV3(ctx, id, version)
	}
}

func (cc *ExecutionClientEngine) getAssembledBlockV3(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	if cc.beaconCfg == nil {
		return nil, nil, nil, nil, errors.New("beaconCfg not set — call SetBeaconChainConfig before GetAssembledBlock")
	}
	resp, err := cc.engine.GetPayloadV3(ctx, hexutil.Bytes(id))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("engine GetPayloadV3 failed: %w", err)
	}
	if resp.ExecutionPayload == nil {
		return nil, nil, nil, nil, errors.New("GetPayloadV3 returned nil execution payload")
	}

	block, err := executionPayloadToEth1Block(resp.ExecutionPayload, version, cc.beaconCfg)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var blockValue *big.Int
	if resp.BlockValue != nil {
		blockValue = resp.BlockValue.ToInt()
	}

	return block, resp.BlobsBundle, nil, blockValue, nil
}

// getAssembledBlockFromResponse converts an Engine response into Caplin's
// block-production values.
func (cc *ExecutionClientEngine) getAssembledBlockFromResponse(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	if resp.ExecutionPayload == nil {
		return nil, nil, nil, nil, errors.New("GetPayload returned nil execution payload")
	}
	if cc.beaconCfg == nil {
		return nil, nil, nil, nil, errors.New("beaconCfg not set — call SetBeaconChainConfig before GetAssembledBlock")
	}

	block, err := executionPayloadToEth1Block(resp.ExecutionPayload, version, cc.beaconCfg)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var blockValue *big.Int
	if resp.BlockValue != nil {
		blockValue = resp.BlockValue.ToInt()
	}

	var requestsBundle *typesproto.RequestsBundle
	if len(resp.ExecutionRequests) > 0 {
		requestsBundle = &typesproto.RequestsBundle{
			Requests: make([][]byte, len(resp.ExecutionRequests)),
		}
		for i, req := range resp.ExecutionRequests {
			requestsBundle.Requests[i] = req
		}
	}

	return block, resp.BlobsBundle, requestsBundle, blockValue, nil
}

func (cc *ExecutionClientEngine) getAssembledBlockV4(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	resp, err := cc.engine.GetPayloadV4(ctx, hexutil.Bytes(id))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("engine GetPayloadV4 failed: %w", err)
	}
	return cc.getAssembledBlockFromResponse(resp, version)
}

func (cc *ExecutionClientEngine) getAssembledBlockV5(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	resp, err := cc.engine.GetPayloadV5(ctx, hexutil.Bytes(id))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("engine GetPayloadV5 failed: %w", err)
	}
	return cc.getAssembledBlockFromResponse(resp, version)
}

func (cc *ExecutionClientEngine) getAssembledBlockV6(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	resp, err := cc.engine.GetPayloadV6(ctx, hexutil.Bytes(id))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("engine GetPayloadV6 failed: %w", err)
	}
	return cc.getAssembledBlockFromResponse(resp, version)
}

func executionPayloadToEth1Block(ep *engine_types.ExecutionPayload, version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) (*cltypes.Eth1Block, error) {
	block := cltypes.NewEth1Block(version, beaconCfg)
	block.ParentHash = ep.ParentHash
	block.FeeRecipient = ep.FeeRecipient
	block.StateRoot = ep.StateRoot
	block.ReceiptsRoot = ep.ReceiptsRoot
	block.PrevRandao = ep.PrevRandao
	block.BlockNumber = uint64(ep.BlockNumber)
	block.GasLimit = uint64(ep.GasLimit)
	block.GasUsed = uint64(ep.GasUsed)
	block.Time = uint64(ep.Timestamp)
	block.BlockHash = ep.BlockHash

	if len(ep.LogsBloom) == 256 {
		copy(block.LogsBloom[:], ep.LogsBloom)
	}

	if ep.ExtraData != nil {
		block.Extra = solid.NewExtraData()
		block.Extra.SetBytes(ep.ExtraData)
	}

	if ep.BaseFeePerGas != nil {
		baseFee := uint256.MustFromBig(ep.BaseFeePerGas.ToInt())
		_, _ = baseFee.MarshalSSZAppend(block.BaseFeePerGas[:0])
	}

	if ep.BlobGasUsed != nil {
		block.BlobGasUsed = uint64(*ep.BlobGasUsed)
	}
	if ep.ExcessBlobGas != nil {
		block.ExcessBlobGas = uint64(*ep.ExcessBlobGas)
	}

	// Keep transactions encoded while rebuilding their SSZ container.
	txBytes := make([][]byte, len(ep.Transactions))
	for i, tx := range ep.Transactions {
		txBytes[i] = tx
	}
	block.Transactions = solid.NewTransactionsSSZFromTransactions(txBytes)

	// Use the chain preset's withdrawal limit when rebuilding the SSZ list.
	if ep.Withdrawals != nil {
		maxWithdrawals := 16
		if beaconCfg != nil {
			maxWithdrawals = int(beaconCfg.MaxWithdrawalsPerPayload)
		}
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](maxWithdrawals, 44)
		for _, w := range ep.Withdrawals {
			block.Withdrawals.Append(&cltypes.Withdrawal{
				Index:     w.Index,
				Validator: w.Validator,
				Address:   w.Address,
				Amount:    w.Amount,
			})
		}
	}

	// Gloas extends the payload with the slot number and block access list.
	if ep.SlotNumber != nil {
		block.SlotNumber = uint64(*ep.SlotNumber)
	}
	if ep.BlockAccessList != nil && len(*ep.BlockAccessList) > 0 {
		maxBytes := uint64(1073741824) // MAX_BYTES_PER_TRANSACTION default
		if beaconCfg != nil {
			maxBytes = beaconCfg.MaxBytesPerTransaction
		}
		block.BlockAccessList = solid.NewByteListSSZ(maxBytes)
		if err := block.BlockAccessList.SetBytes(*ep.BlockAccessList); err != nil {
			return nil, err
		}
	}

	return block, nil
}

func (cc *ExecutionClientEngine) HasGapInSnapshots(ctx context.Context) bool {
	if cc.isLocal() {
		_, hasGap := cc.chainRW.FrozenBlocks(ctx)
		return hasGap
	}
	return false
}

func (cc *ExecutionClientEngine) GetBlobs(ctx context.Context, versionedHashes []common.Hash, version clparams.StateVersion) (blobs [][]byte, proofs [][][]byte, err error) {
	if cc.isLocal() && cc.txpool != nil {
		req := &txpoolproto.GetBlobsRequest{BlobHashes: make([]*typesproto.H256, len(versionedHashes))}
		for i, h := range versionedHashes {
			req.BlobHashes[i] = gointerfaces.ConvertHashToH256(h)
		}
		resp, err := cc.txpool.GetBlobs(ctx, req)
		if err != nil {
			if errors.Is(err, txpool.ErrPoolDisabled) {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("txpool GetBlobs: %w", err)
		}
		blobsWithProof := resp.BlobsWithProofs
		blobs = make([][]byte, len(blobsWithProof))
		proofs = make([][][]byte, len(blobsWithProof))
		for i, bwp := range blobsWithProof {
			blobs[i] = bwp.Blob
			proofs[i] = bwp.Proofs
		}
		return blobs, proofs, nil
	}

	// Deneb and Electra expect one proof per blob. Fulu and later expect cell
	// proofs.
	if version >= clparams.FuluVersion {
		result, err := cc.engine.GetBlobsV2(ctx, versionedHashes)
		if err != nil {
			return nil, nil, fmt.Errorf("engine GetBlobsV2: %w", err)
		}
		blobs = make([][]byte, len(result))
		proofs = make([][][]byte, len(result))
		for i, bap := range result {
			if bap == nil {
				continue
			}
			blobs[i] = bap.Blob
			proofs[i] = make([][]byte, len(bap.CellProofs))
			for j, cp := range bap.CellProofs {
				proofs[i][j] = cp
			}
		}
		return blobs, proofs, nil
	}

	result, err := cc.engine.GetBlobsV1(ctx, versionedHashes)
	if err != nil {
		return nil, nil, fmt.Errorf("engine GetBlobsV1: %w", err)
	}
	blobs = make([][]byte, len(result))
	proofs = make([][][]byte, len(result))
	for i, bap := range result {
		if bap == nil {
			continue
		}
		blobs[i] = bap.Blob
		proofs[i] = [][]byte{bap.Proof}
	}
	return blobs, proofs, nil
}

func (cc *ExecutionClientEngine) GetClientVersionV1(ctx context.Context, callerVersion *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
	return cc.engine.GetClientVersionV1(ctx, callerVersion)
}
