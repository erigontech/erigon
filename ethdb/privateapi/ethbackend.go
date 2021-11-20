package privateapi

import (
	"bytes"
	"context"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/ledgerwatch/secp256k1"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	Syncing = "SYNCING"
	Valid   = "VALID"
	Invalid = "INVALID"
)

// EthBackendAPIVersion
// 2.0.0 - move all mining-related methods to 'txpool/mining' server
// 2.1.0 - add NetPeerCount function
var EthBackendAPIVersion = &types2.VersionReply{Major: 2, Minor: 1, Patch: 0}

type EthBackendServer struct {
	remote.UnimplementedETHBACKENDServer // must be embedded to have forward compatible implementations.

	ctx           context.Context
	eth           EthBackend
	events        *Events
	db            kv.RwDB
	blockReader   *snapshotsync.BlockReader
	config        *params.ChainConfig
	executeConfig stagedsync.ExecuteBlockCfg
	vmConfig      vm.Config
}

type EthBackend interface {
	Etherbase() (common.Address, error)
	NetVersion() (uint64, error)
	NetPeerCount() (uint64, error)
}

func NewEthBackendServer(ctx context.Context, eth EthBackend, db kv.RwDB, config *params.ChainConfig, executeConfig stagedsync.ExecuteBlockCfg, vmConfig vm.Config, events *Events) *EthBackendServer {
	return &EthBackendServer{ctx: ctx, eth: eth, events: events,
		config: config, db: db, executeConfig: executeConfig,
		vmConfig: vmConfig, blockReader: snapshotsync.NewBlockReader(),
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
	// Check mandatory fields
	if req.ParentHash == nil || req.BlockHash == nil || req.Coinbase == nil || req.ExtraData == nil ||
		req.LogsBloom == nil || req.ReceiptRoot == nil || req.StateRoot == nil || req.Random == nil ||
		s.config.TerminalTotalDifficulty == nil {

		return &remote.EngineExecutePayloadReply{
			Status: Invalid,
		}, nil
	}

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	// If another payload is already commissioned then we just reply with syncing
	_, _, found, err := rawdb.ReadPayload(tx)
	if err != nil {
		return nil, err
	}

	currentHead := rawdb.ReadHeadBlockHash(tx)
	blockHash := gointerfaces.ConvertH256ToHash(req.BlockHash)

	if found {
		// We are still syncing a commisioned payload
		return &remote.EngineExecutePayloadReply{
			Status:          Syncing,
			LatestValidHash: gointerfaces.ConvertHashToH256(currentHead),
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
		Random:      gointerfaces.ConvertH256ToHash(req.Random),
		Eip3675:     true,
		Eip1559:     eip1559,
		BaseFee:     baseFee,
		Extra:       extra_data.Bytes(),
		Number:      big.NewInt(int64(req.BlockNumber)),
		GasUsed:     req.GasUsed,
		GasLimit:    req.GasLimit,
		Time:        req.Timestamp,
		ReceiptHash: gointerfaces.ConvertH256ToHash(req.ReceiptRoot),
	}
	// Our execution layer has some problems so we return invalid
	if header.Hash() != blockHash {
		return &remote.EngineExecutePayloadReply{
			Status:          Invalid,
			LatestValidHash: gointerfaces.ConvertHashToH256(currentHead),
		}, nil
	}
	// Check if current block is next for execution, if not, commission it and start
	// Reverse-download the chain from its block number and hash.
	if header.ParentHash != currentHead {
		// We commissioned a payload and now we are syncing
		return &remote.EngineExecutePayloadReply{
			Status:          Syncing,
			LatestValidHash: gointerfaces.ConvertHashToH256(currentHead),
		}, rawdb.CommissionPayload(tx, req.BlockNumber, blockHash)
	}

	// Decode transactions
	reader := bytes.NewReader(nil)
	stream := rlp.NewStream(reader, 0)
	transactions := make([]types.Transaction, len(req.Transactions))

	for i, encodedTransaction := range req.Transactions {
		reader.Reset(encodedTransaction)
		stream.Reset(reader, 0)
		if transactions[i], err = types.DecodeTransaction(stream); err != nil {
			return nil, err
		}
		i++
	}
	// Write Header(we do not need to add difficulty after "The merge")
	rawdb.WriteHeader(tx, &header)
	if err := rawdb.WriteCanonicalHash(tx, blockHash, req.BlockNumber); err != nil {
		return nil, err
	}
	rawdb.WriteHeaderNumber(tx, blockHash, req.BlockNumber)
	// Write Body
	if err := rawdb.WriteRawBody(tx, blockHash, req.BlockNumber, &types.RawBody{
		Transactions: req.Transactions,
	}); err != nil {
		return nil, err
	}
	// Senders recovery
	signer := types.MakeSigner(s.config, req.BlockNumber)
	senders := make([]common.Address, len(req.Transactions))
	for i, tx := range transactions {
		senders[i], err = signer.SenderWithContext(secp256k1.NewContext(), tx)
		if err != nil {
			return nil, err
		}
	}
	if err := rawdb.WriteSenders(tx, blockHash, req.BlockNumber, senders); err != nil {
		return nil, err
	}
	block := types.NewBlock(&header, transactions, []*types.Header{}, []*types.Receipt{})
	// Create batch
	var batch ethdb.DbWithPendingMutations
	batch = olddb.NewBatch(tx, ctx.Done())
	defer batch.Rollback()
	// TEVM handler
	var contractHasTEVM func(contractHash common.Hash) (bool, error)

	if s.vmConfig.EnableTEMV {
		contractHasTEVM = ethdb.GetHasTEVM(tx)
	}
	// Execute the payload
	if err := stagedsync.ExecuteBlock(block, tx, batch, s.executeConfig, s.vmConfig, true, true, true, contractHasTEVM, false); err != nil {
		return &remote.EngineExecutePayloadReply{
			Status: Invalid,
		}, nil
	}

	// Update Progress
	if err := stages.SaveStageProgress(tx, stages.Headers, req.BlockNumber); err != nil {
		return nil, err
	}
	if err := stages.SaveStageProgress(tx, stages.BlockHashes, req.BlockNumber); err != nil {
		return nil, err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, req.BlockNumber); err != nil {
		return nil, err
	}
	if err := stages.SaveStageProgress(tx, stages.Senders, req.BlockNumber); err != nil {
		return nil, err
	}
	if err := stages.SaveStageProgress(tx, stages.Execution, req.BlockNumber); err != nil {
		return nil, err
	}
	return nil, nil
}
