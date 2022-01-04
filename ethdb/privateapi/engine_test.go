package privateapi

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
)

// Hashes
var (
	startingHeadHash = common.HexToHash("0x1")
	payload1Hash     = common.HexToHash("2afc6f4132be8d1fded51aa7f914fd831d2939a100f61322842ab41d7898255b")
	payload2Hash     = common.HexToHash("a19013b8b5f95ffaa008942fd2f04f72a3ae91f54eb64a3f6f8c6630db742aef")
	payload3Hash     = common.HexToHash("a2dd4fc599747c1ce2176a4abae13afbc7ccb4a240f8f4cf22252767bab52f12")
)

// Payloads
var (
	mockPayload1 *types2.ExecutionPayload = &types2.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(common.HexToHash("0x2")),
		BlockHash:     gointerfaces.ConvertHashToH256(payload1Hash),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
		StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
		Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
		ExtraData:     make([]byte, 0),
		BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		BlockNumber:   100,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
		Transactions:  make([][]byte, 0),
	}
	mockPayload2 *types2.ExecutionPayload = &types2.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(payload1Hash),
		BlockHash:     gointerfaces.ConvertHashToH256(payload2Hash),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
		StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
		Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
		ExtraData:     make([]byte, 0),
		BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		BlockNumber:   101,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
		Transactions:  make([][]byte, 0),
	}
	mockPayload3 = &types2.ExecutionPayload{
		ParentHash:    gointerfaces.ConvertHashToH256(startingHeadHash),
		BlockHash:     gointerfaces.ConvertHashToH256(payload3Hash),
		ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
		StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
		Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
		ExtraData:     make([]byte, 0),
		BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		BlockNumber:   51,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
		Transactions:  make([][]byte, 0),
	}
)

func makeTestDb(ctx context.Context, db kv.RwDB) {
	tx, _ := db.BeginRw(ctx)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)
	_ = tx.Commit()
}

func TestMockDownloadRequest(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)
	reverseDownloadCh := make(chan PayloadMessage)
	statusCh := make(chan ExecutionStatus)
	waitingForHeaders := uint32(1)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh, &waitingForHeaders, nil, nil, false)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload1)
		done <- true
	}()

	<-reverseDownloadCh
	statusCh <- ExecutionStatus{Status: Syncing}
	atomic.StoreUint32(&waitingForHeaders, 0)
	<-done
	require.NoError(err)
	require.Equal(reply.Status, string(Syncing))
	require.Nil(reply.LatestValidHash)

	// If we get another request we don't need to process it with processDownloadCh and ignore it and return Syncing status
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload2)
		done <- true
	}()

	<-done
	// Same result as before
	require.NoError(err)
	require.Equal(reply.Status, string(Syncing))
	require.Nil(reply.LatestValidHash)

	// However if we simulate that we finish reverse downloading the chain by updating the head, we just execute 1:1
	tx, _ := db.BeginRw(ctx)
	rawdb.WriteHeadBlockHash(tx, payload1Hash)
	rawdb.WriteHeaderNumber(tx, payload1Hash, 100)
	_ = tx.Commit()
	// Now we try to sync the next payload again
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload2)
		done <- true
	}()

	<-done

	require.NoError(err)
	require.Equal(reply.Status, string(Syncing))
	require.Nil(reply.LatestValidHash)
}

func TestMockValidExecution(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)

	reverseDownloadCh := make(chan PayloadMessage)
	statusCh := make(chan ExecutionStatus)
	waitingForHeaders := uint32(1)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh, &waitingForHeaders, nil, nil, false)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload3)
		done <- true
	}()

	<-reverseDownloadCh

	statusCh <- ExecutionStatus{
		Status:          Valid,
		LatestValidHash: payload3Hash,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, string(Valid))
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], payload3Hash[:])
}

func TestMockInvalidExecution(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)

	reverseDownloadCh := make(chan PayloadMessage)
	statusCh := make(chan ExecutionStatus)

	waitingForHeaders := uint32(1)
	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh, &waitingForHeaders, nil, nil, false)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload3)
		done <- true
	}()

	<-reverseDownloadCh
	// Simulate invalid status
	statusCh <- ExecutionStatus{
		Status:          Invalid,
		LatestValidHash: startingHeadHash,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, string(Invalid))
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], startingHeadHash[:])
}

func TestNoTTD(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)

	reverseDownloadCh := make(chan PayloadMessage)
	statusCh := make(chan ExecutionStatus)
	waitingForHeaders := uint32(1)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{}, reverseDownloadCh, statusCh, &waitingForHeaders, nil, nil, false)

	var err error

	done := make(chan bool)

	go func() {
		_, err = backend.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
			ParentHash:    gointerfaces.ConvertHashToH256(common.HexToHash("0x2")),
			BlockHash:     gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
			ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
			ExtraData:     make([]byte, 0),
			BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			BlockNumber:   51,
			GasLimit:      52,
			GasUsed:       4,
			Timestamp:     4,
			Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
			Transactions:  make([][]byte, 0),
		})
		done <- true
	}()

	<-done

	require.Equal(err.Error(), "not a proof-of-stake chain")
}
