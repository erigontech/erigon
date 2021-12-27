package privateapi

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PayloadStatus string

const (
	Syncing PayloadStatus = "SYNCING"
	Valid   PayloadStatus = "VALID"
	Invalid PayloadStatus = "INVALID"
)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
// 2.2.0 - add NodesInfo function
// 3.0.0 - adding PoS interfaces
var EthBackendAPIVersion = &types2.VersionReply{Major: 3, Minor: 0, Patch: 0}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx         context.Context
	eth         EthBackend
	events      *Events
	db          kv.RoDB
	blockReader interfaces.BlockReader
	config      *params.ChainConfig
	// Block proposing for proof-of-stake
	nextPayloadId   uint64
	pendingPayloads map[uint64]types2.ExecutionPayload
	// Send reverse sync starting point to staged sync
	reverseDownloadCh chan<- PayloadMessage
	// Notify whether the current block being processed is Valid or not
	statusCh <-chan ExecutionStatus
	// Last block number sent over via reverseDownloadCh
	numberSent uint64
	// Determines whether stageloop is processing a block or not
	waitingForBeaconChain *uint32           // atomic boolean flag
	assemblePayloadFunc   assembleStartFunc // starts assembling payload
	mu                    sync.Mutex
}

type EthBackend interface {
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
	NodesInfo(limit int) (*remote.NodesInfoReply, error)
}

type assembleStartFunc func(timestamp uint64, random common.Hash) (types.Block, error)

// This is the status of a newly execute block.
// Hash: Block hash
// Status: block's status
type ExecutionStatus struct {
	Status          PayloadStatus
	LatestValidHash common.Hash
	Error           error
}

// The message we are going to send to the stage sync in ExecutePayload
type PayloadMessage struct {
	Header *types.Header
	Body   *types.RawBody
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *Events, blockReader interfaces.BlockReader,
	config *params.ChainConfig, reverseDownloadCh chan<- PayloadMessage, statusCh <-chan ExecutionStatus, waitingForBeaconChain *uint32,
	assemblePayloadFunc assembleStartFunc,
) *EthBackendServer {
	return &EthBackendServer{ctx: ctx, eth: eth, events: events, db: db, blockReader: blockReader, config: config,
		reverseDownloadCh: reverseDownloadCh, statusCh: statusCh, waitingForBeaconChain: waitingForBeaconChain,
		pendingPayloads: make(map[uint64]types2.ExecutionPayload), assemblePayloadFunc: assemblePayloadFunc,
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

func (s *EthBackendServer) Subscribe(r *remote.SubscribeRequest, subscribeServer remote.ETHBACKEND_SubscribeServer) error {
	log.Trace("Establishing event subscription channel with the RPC daemon ...")
	s.events.AddHeaderSubscription(func(h *types.Header) error {
		select {
		case <-s.ctx.Done():
			return nil
		case <-subscribeServer.Context().Done():
			return nil
		default:
		}

		var buf bytes.Buffer
		if err := rlp.Encode(&buf, h); err != nil {
			log.Warn("error while marshaling a header", "err", err)
			return err
		}
		payload := buf.Bytes()

		err := subscribeServer.Send(&remote.SubscribeReply{
			Type: remote.Event_HEADER,
			Data: payload,
		})

		// we only close the wg on error because if we successfully sent an event,
		// that means that the channel wasn't closed and is ready to
		// receive more events.
		// if rpcdaemon disconnects, we will receive an error here
		// next time we try to send an event
		if err != nil {
			log.Info("event subscription channel was closed", "reason", err)
		}
		return err
	})

	log.Info("event subscription channel established with the RPC daemon")
	select {
	case <-subscribeServer.Context().Done():
	case <-s.ctx.Done():
	}
	log.Info("event subscription channel closed with the RPC daemon")
	return nil
}

func (s *EthBackendServer) ProtocolVersion(_ context.Context, _ *remote.ProtocolVersionRequest) (*remote.ProtocolVersionReply, error) {
	// Hardcoding to avoid import cycle
	return &remote.ProtocolVersionReply{Id: 66}, nil
}

func (s *EthBackendServer) ClientVersion(_ context.Context, _ *remote.ClientVersionRequest) (*remote.ClientVersionReply, error) {
	return &remote.ClientVersionReply{NodeName: common.MakeName("erigon", params.Version)}, nil
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
	for i := range senders {
		sendersBytes = append(sendersBytes, senders[i][:]...)
	}
	return &remote.BlockReply{BlockRlp: blockRlp, Senders: sendersBytes}, nil
}

// EngineExecutePayloadV1, executes payload
func (s *EthBackendServer) EngineExecutePayloadV1(ctx context.Context, req *types2.ExecutionPayload) (*remote.EngineExecutePayloadReply, error) {

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)
	// Discard all previous prepared payloads if another block was proposed
	s.nextPayloadId = 0
	s.pendingPayloads = make(map[uint64]types2.ExecutionPayload)
	// If another payload is already commissioned then we just reply with syncing
	if atomic.LoadUint32(s.waitingForBeaconChain) == 0 {
		// We are still syncing a commissioned payload
		return &remote.EngineExecutePayloadReply{Status: string(Syncing)}, nil
	}
	// Let's check if we have parent hash, if we have it we can process the payload right now.
	// If not, we need to commission it and reverse-download the chain.
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
		TxHash:      types.DeriveSha(types.RawTransactions(req.Transactions)),
	}
	// Our execution layer has some problems so we return invalid
	if header.Hash() != blockHash {
		return nil, fmt.Errorf("invalid hash for payload. got: %s, wanted: %s", common.Bytes2Hex(blockHash[:]), common.Bytes2Hex(header.Hash().Bytes()))
	}
	// Send the block over
	s.numberSent = req.BlockNumber
	s.reverseDownloadCh <- PayloadMessage{
		Header: &header,
		Body: &types.RawBody{
			Transactions: req.Transactions,
			Uncles:       nil,
		},
	}

	executedStatus := <-s.statusCh

	if executedStatus.Error != nil {
		return nil, executedStatus.Error
	}

	reply := remote.EngineExecutePayloadReply{Status: string(executedStatus.Status)}
	if executedStatus.LatestValidHash != (common.Hash{}) {
		reply.LatestValidHash = gointerfaces.ConvertHashToH256(executedStatus.LatestValidHash)
	}
	return &reply, nil
}

// EngineGetPayloadV1, retrieves previously assembled payload (Validators only)
func (s *EthBackendServer) EngineGetPayloadV1(ctx context.Context, req *remote.EngineGetPayloadRequest) (*types2.ExecutionPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}

	payload, ok := s.pendingPayloads[req.PayloadId]
	if ok {
		return &payload, nil
	}
	return nil, fmt.Errorf("unknown payload")
}

// EngineGetPayloadV1, retrieves previously assembled payload (Validators only)
func (s *EthBackendServer) EngineForkChoiceUpdatedV1(ctx context.Context, req *remote.EngineForkChoiceUpdatedRequest) (*remote.EngineForkChoiceUpdatedReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.config.TerminalTotalDifficulty == nil {
		return nil, fmt.Errorf("not a proof-of-stake chain")
	}
	// Check if parent equate to the head
	parent := gointerfaces.ConvertH256ToHash(req.Forkchoice.HeadBlockHash)
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	random := gointerfaces.ConvertH256ToHash(req.Prepare.Random)
	block, err := s.assemblePayloadFunc(req.Prepare.Timestamp, random)
	if err != nil {
		return nil, err
	}

	if parent != rawdb.ReadHeadBlockHash(tx) || block.Header().ParentHash != parent || atomic.LoadUint32(s.waitingForBeaconChain) == 0 {
		return &remote.EngineForkChoiceUpdatedReply{
			Status: string(Syncing),
		}, nil
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

		err := rlp.Encode(buf, tx)
		if err != nil {
			return nil, fmt.Errorf("broken tx rlp: %w", err)
		}
		encodedTransactions = append(encodedTransactions, common.CopyBytes(buf.Bytes()))
	}
	// Set parameters accordingly to what the beacon chain told us and from what the mining stage told us
	s.pendingPayloads[s.nextPayloadId] = types2.ExecutionPayload{
		ParentHash:    req.Forkchoice.HeadBlockHash,
		Coinbase:      gointerfaces.ConvertAddressToH160(block.Header().Coinbase),
		Timestamp:     req.Prepare.Timestamp,
		Random:        req.Prepare.Random,
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
	// successfully assembled the payload and assinged the correct id
	defer func() { s.nextPayloadId++ }()
	return &remote.EngineForkChoiceUpdatedReply{
		Status:    "SUCCESS",
		PayloadId: s.nextPayloadId,
	}, nil
}

func (s *EthBackendServer) NodeInfo(_ context.Context, r *remote.NodesInfoRequest) (*remote.NodesInfoReply, error) {
	nodesInfo, err := s.eth.NodesInfo(int(r.Limit))
	if err != nil {
		return nil, err
	}
	return nodesInfo, nil
}
