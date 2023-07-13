package engineapi

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
)

// Hashes
var (
	startingHeadHash = libcommon.HexToHash("0x1")
	payload1Hash     = libcommon.HexToHash("2afc6f4132be8d1fded51aa7f914fd831d2939a100f61322842ab41d7898255b")
	payload2Hash     = libcommon.HexToHash("219bb787708f832e40b734ab925e67e33f91f2c2a2a01b8f5f5f6284410982a6")
	payload3Hash     = libcommon.HexToHash("a2dd4fc599747c1ce2176a4abae13afbc7ccb4a240f8f4cf22252767bab52f12")
)

// Payloads
var (
	mockPayload1 = &engine_types.ExecutionPayload{
		ParentHash:    libcommon.HexToHash("0x2"),
		BlockHash:     payload1Hash,
		ReceiptsRoot:  libcommon.HexToHash("0x3"),
		StateRoot:     libcommon.HexToHash("0x4"),
		PrevRandao:    libcommon.HexToHash("0x0b3"),
		LogsBloom:     make([]byte, 256),
		BaseFeePerGas: (*hexutil.Big)(big.NewInt(0x0b3)),
		BlockNumber:   100,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		FeeRecipient:  libcommon.HexToAddress("0x1"),
	}
	mockPayload2 = &engine_types.ExecutionPayload{
		ParentHash:    payload1Hash,
		BlockHash:     payload2Hash,
		ReceiptsRoot:  libcommon.HexToHash("0x3"),
		StateRoot:     libcommon.HexToHash("0x4"),
		PrevRandao:    libcommon.HexToHash("0x0b3"),
		LogsBloom:     make([]byte, 256),
		BaseFeePerGas: (*hexutil.Big)(big.NewInt(0x0b3)),
		BlockNumber:   101,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		FeeRecipient:  libcommon.HexToAddress("0x1"),
	}
	mockPayload3 = &engine_types.ExecutionPayload{
		ParentHash:    startingHeadHash,
		BlockHash:     payload3Hash,
		ReceiptsRoot:  libcommon.HexToHash("0x3"),
		StateRoot:     libcommon.HexToHash("0x4"),
		PrevRandao:    libcommon.HexToHash("0x0b3"),
		LogsBloom:     make([]byte, 256),
		BaseFeePerGas: (*hexutil.Big)(big.NewInt(0x0b3)),
		BlockNumber:   51,
		GasLimit:      52,
		GasUsed:       4,
		Timestamp:     4,
		FeeRecipient:  libcommon.HexToAddress("0x1"),
	}
)

func makeTestDb(ctx context.Context, db kv.RwDB) {
	tx, _ := db.BeginRw(ctx)
	rawdb.WriteHeadBlockHash(tx, startingHeadHash)
	rawdb.WriteHeaderNumber(tx, startingHeadHash, 50)
	_ = tx.Commit()
}

func TestMockDownloadRequest(t *testing.T) {
	logger := log.New()
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)
	hd := headerdownload.NewHeaderDownload(0, 0, nil, nil, logger)
	hd.SetPOSSync(true)
	backend := NewEngineServer(ctx, logger, &chain.Config{TerminalTotalDifficulty: libcommon.Big1}, nil, db, nil, hd, false)

	var err error
	var reply *engine_types.PayloadStatus
	done := make(chan bool)

	go func() {
		reply, err = backend.NewPayloadV1(ctx, mockPayload1)
		done <- true
	}()

	hd.BeaconRequestList.WaitForRequest(true, false)
	hd.PayloadStatusCh <- engine_types.PayloadStatus{Status: engine_types.SyncingStatus}
	<-done
	require.NoError(err)
	require.Equal(reply.Status, engine_types.SyncingStatus)
	require.Nil(reply.LatestValidHash)

	// If we get another request we don't need to process it with processDownloadCh and ignore it and return Syncing status
	go func() {
		reply, err = backend.NewPayloadV1(ctx, mockPayload2)
		done <- true
	}()

	<-done
	// Same result as before
	require.NoError(err)
	require.Equal(reply.Status, engine_types.SyncingStatus)
	require.Nil(reply.LatestValidHash)

	// However if we simulate that we finish reverse downloading the chain by updating the head, we just execute 1:1
	tx, _ := db.BeginRw(ctx)
	rawdb.WriteHeadBlockHash(tx, payload1Hash)
	rawdb.WriteHeaderNumber(tx, payload1Hash, 100)
	_ = tx.Commit()
	// Now we try to sync the next payload again
	go func() {
		reply, err = backend.NewPayloadV1(ctx, mockPayload2)
		done <- true
	}()

	<-done

	require.NoError(err)
	require.Equal(reply.Status, engine_types.SyncingStatus)
	require.Nil(reply.LatestValidHash)
}

func TestMockValidExecution(t *testing.T) {
	logger := log.New()
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)

	hd := headerdownload.NewHeaderDownload(0, 0, nil, nil, logger)
	hd.SetPOSSync(true)

	backend := NewEngineServer(ctx, logger, &chain.Config{TerminalTotalDifficulty: libcommon.Big1}, nil, db, nil, hd, false)

	var err error
	var reply *engine_types.PayloadStatus

	done := make(chan bool)

	go func() {
		reply, err = backend.NewPayloadV1(ctx, mockPayload3)
		done <- true
	}()

	hd.BeaconRequestList.WaitForRequest(true, false)

	hd.PayloadStatusCh <- engine_types.PayloadStatus{
		Status:          engine_types.ValidStatus,
		LatestValidHash: &payload3Hash,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, engine_types.ValidStatus)
	replyHash := reply.LatestValidHash
	require.Equal(replyHash[:], payload3Hash[:])
}

func TestMockInvalidExecution(t *testing.T) {
	logger := log.New()
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)

	hd := headerdownload.NewHeaderDownload(0, 0, nil, nil, logger)
	hd.SetPOSSync(true)

	backend := NewEngineServer(ctx, logger, &chain.Config{TerminalTotalDifficulty: libcommon.Big1}, nil, db, nil, hd, false)

	var err error
	var reply *engine_types.PayloadStatus
	done := make(chan bool)

	go func() {
		reply, err = backend.NewPayloadV1(ctx, mockPayload3)
		done <- true
	}()

	hd.BeaconRequestList.WaitForRequest(true, false)
	// Simulate invalid status
	hd.PayloadStatusCh <- engine_types.PayloadStatus{
		Status:          engine_types.InvalidStatus,
		LatestValidHash: &startingHeadHash,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, engine_types.InvalidStatus)
	replyHash := reply.LatestValidHash
	require.Equal(replyHash[:], startingHeadHash[:])
}

func TestNoTTD(t *testing.T) {
	logger := log.New()
	db := memdb.NewTestDB(t)
	ctx := context.Background()
	require := require.New(t)

	makeTestDb(ctx, db)

	hd := headerdownload.NewHeaderDownload(0, 0, nil, nil, logger)

	backend := NewEngineServer(ctx, logger, &chain.Config{}, nil, db, nil, hd, false)

	var err error

	done := make(chan bool)

	go func() {
		_, err = backend.NewPayloadV1(ctx, &engine_types.ExecutionPayload{
			BlockHash:     libcommon.HexToHash("0x809f6ec0f4fff2bcbb83d1a0c82f59200c531a5e220c68dd9603b17485375c40"),
			BaseFeePerGas: (*hexutil.Big)(big.NewInt(0x0b3)),
		})
		done <- true
	}()

	<-done

	require.Equal(err.Error(), "not a proof-of-stake chain")
}
