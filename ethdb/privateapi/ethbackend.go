package privateapi

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

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

	ctx             context.Context
	eth             EthBackend
	events          *Events
	db              kv.RoDB
	blockReader     interfaces.BlockReader
	config          *params.ChainConfig
	pendingPayloads map[uint64]types2.ExecutionPayload
	// Send reverse sync starting point to staged sync
	reverseDownloadCh chan<- types.Header
	// Notify whether the current block being processed is Valid or not
	statusCh <-chan ExecutionStatus
	// Last block number sent over via reverseDownloadCh
	numberSent uint64
	latestHead common.Hash // The last head processed through ethbackend
	// Determines whether stageloop is processing a block or not
	waitingForPOSHeaders *uint32 // atomic boolean flag
	mu                   sync.Mutex
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
type ExecutionStatus struct {
	HeadHash common.Hash
	Status   PayloadStatus
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, events *Events, blockReader interfaces.BlockReader,
	config *params.ChainConfig, reverseDownloadCh chan<- types.Header, statusCh <-chan ExecutionStatus, waitingForPOSHeaders *uint32,
) *EthBackendServer {
	return &EthBackendServer{ctx: ctx, eth: eth, events: events, db: db, blockReader: blockReader, config: config,
		reverseDownloadCh: reverseDownloadCh, statusCh: statusCh, waitingForPOSHeaders: waitingForPOSHeaders,
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

	// If another payload is already commissioned then we just reply with syncing
	if atomic.LoadUint32(s.waitingForPOSHeaders) == 0 {
		// We are still syncing a commissioned payload
		return &remote.EngineExecutePayloadReply{
			Status:          string(Syncing),
			LatestValidHash: gointerfaces.ConvertHashToH256(s.latestHead),
		}, nil
	}
	// Let's check if we have parent hash, if we have it we can process the payload right now.
	// If not, we need to commission it and reverse-download the chain.
	var baseFee *big.Int
	eip1559 := false

	if req.BaseFeePerGas != nil {
		baseFee = gointerfaces.ConvertH256ToUint256Int(req.BaseFeePerGas).ToBig()
		eip1559 = true
	}

	// Extra data can go from 0 to 32 bytes, so it can be treated as an hash
	var extra_data common.Hash = gointerfaces.ConvertH256ToHash(req.ExtraData)
	header := types.Header{
		ParentHash:  gointerfaces.ConvertH256ToHash(req.ParentHash),
		Coinbase:    gointerfaces.ConvertH160toAddress(req.Coinbase),
		Root:        gointerfaces.ConvertH256ToHash(req.StateRoot),
		Bloom:       gointerfaces.ConvertH2048ToBloom(req.LogsBloom),
		Eip1559:     eip1559,
		BaseFee:     baseFee,
		Extra:       extra_data.Bytes(),
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
	s.reverseDownloadCh <- header

	executedStatus := <-s.statusCh

	s.latestHead = executedStatus.HeadHash
	return &remote.EngineExecutePayloadReply{
		Status:          string(executedStatus.Status),
		LatestValidHash: gointerfaces.ConvertHashToH256(executedStatus.HeadHash),
	}, nil
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

func (s *EthBackendServer) NodeInfo(_ context.Context, r *remote.NodesInfoRequest) (*remote.NodesInfoReply, error) {
	nodesInfo, err := s.eth.NodesInfo(int(r.Limit))
	if err != nil {
		return nil, err
	}
	return nodesInfo, nil
}
