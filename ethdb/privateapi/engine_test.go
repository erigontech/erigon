package privateapi

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
)

// Hashes
var (
	startingHeadHash = common.HexToHash("0x1")
	payload1Hash     = common.HexToHash("9c344b6c65e0293dd3d45510fef3b6e957a1058c124d88ff607aa59fbd1293a7")
	payload2Hash     = common.HexToHash("99937164b496a50595aa445a587200fd8a84024e63078871ab3508b6483a8c53")
	payload3Hash     = common.HexToHash("34f005ca6dee15d637b2c45c3fca124a479daa7bc25c781bba6782ce90aac566")
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
		ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
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
		ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
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
		ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
		BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
		BlockNumber:   51,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
		Transactions:  make([][]byte, 0),
	}
)

func TestMockDownloadRequest(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload1)
		done <- true
	}()

	<-reverseDownloadCh

	<-done

	require.NoError(err)
	require.Equal(reply.Status, Syncing)
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], startingHeadHash[:])

	// If we get another request we dont need to process it with processDownloadCh and ignore it and return Syncing status
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload2)
		done <- true
	}()

	<-done
	// Same result as before
	require.NoError(err)
	require.Equal(reply.Status, Syncing)
	replyHash = gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], startingHeadHash[:])

	// However if we simulate that we finish reverse downloading the chain by updating the head, we just execute 1:1
	tx, _ = db.BeginRw(ctx)
	rawdb.WriteHeadBlockHash(tx, payload1Hash)
	rawdb.WriteHeaderNumber(tx, payload1Hash, 100)
	_ = tx.Commit()
	// Now we try to sync the next payload again
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload2)
		done <- true
	}()

	<-reverseDownloadCh

	statusCh <- core.ExecutionStatus{
		Hash:  payload2Hash,
		Valid: true,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, Valid)
	replyHash = gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], payload2Hash[:])
}

func TestMockValidExecution(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload3)
		done <- true
	}()

	<-reverseDownloadCh

	statusCh <- core.ExecutionStatus{
		Hash:  payload3Hash,
		Valid: true,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, Valid)
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], payload3Hash[:])
}

func TestMockInvalidExecution(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, mockPayload3)
		done <- true
	}()

	<-reverseDownloadCh

	statusCh <- core.ExecutionStatus{
		Hash:  payload3Hash,
		Valid: false,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, Invalid)
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], startingHeadHash[:])
}

func TestInvalidRequest(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh)

	var err error

	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		_, err = backend.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
			BlockHash:     gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
			ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
			ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
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

	require.Equal(err.Error(), "invalid execution payload")
}

func TestNoTTD(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{}, reverseDownloadCh, statusCh)

	var err error

	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		_, err = backend.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
			ParentHash:    gointerfaces.ConvertHashToH256(common.HexToHash("0x2")),
			BlockHash:     gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
			ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
			ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
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
