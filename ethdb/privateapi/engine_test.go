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

func TestMockValidExecution(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	head := common.HexToHash("0x1")
	rawdb.WriteHeadBlockHash(tx, head)
	rawdb.WriteHeaderNumber(tx, head, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
			ParentHash:    gointerfaces.ConvertHashToH256(head),
			BlockHash:     gointerfaces.ConvertHashToH256(common.HexToHash("f828f16f055129d0b0850c5c463b19e3c0c09c7b1a64a597b3bc00c0abb49346")),
			ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
			StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
			ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
			BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			BlockNumber:   2,
			GasLimit:      52,
			GasUsed:       4,
			Timestamp:     4,
			Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
			Transactions:  make([][]byte, 0),
		})
		done <- true
	}()

	<-reverseDownloadCh

	statusCh <- core.ExecutionStatus{
		Hash:  common.HexToHash("7088846e2055286ec5a95ce9f79b5d8877d228313e19ff936b4ebdfae393e3c5"),
		Valid: true,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, Valid)
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], common.HexToHash("f828f16f055129d0b0850c5c463b19e3c0c09c7b1a64a597b3bc00c0abb49346").Bytes())
}

func TestMockInvalidExecution(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	head := common.HexToHash("0x1")
	rawdb.WriteHeadBlockHash(tx, head)
	rawdb.WriteHeaderNumber(tx, head, 50)

	reverseDownloadCh := make(chan types.Block)
	statusCh := make(chan core.ExecutionStatus)

	backend := NewEthBackendServer(ctx, nil, db, nil, nil, &params.ChainConfig{TerminalTotalDifficulty: common.Big1}, reverseDownloadCh, statusCh)

	var err error
	var reply *remote.EngineExecutePayloadReply
	done := make(chan bool)

	_ = tx.Commit()
	go func() {
		reply, err = backend.EngineExecutePayloadV1(ctx, &types2.ExecutionPayload{
			ParentHash:    gointerfaces.ConvertHashToH256(head),
			BlockHash:     gointerfaces.ConvertHashToH256(common.HexToHash("f828f16f055129d0b0850c5c463b19e3c0c09c7b1a64a597b3bc00c0abb49346")),
			ReceiptRoot:   gointerfaces.ConvertHashToH256(common.HexToHash("0x3")),
			StateRoot:     gointerfaces.ConvertHashToH256(common.HexToHash("0x4")),
			Random:        gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
			ExtraData:     gointerfaces.ConvertHashToH256(common.Hash{}),
			BaseFeePerGas: gointerfaces.ConvertHashToH256(common.HexToHash("0x0b3")),
			BlockNumber:   2,
			GasLimit:      52,
			GasUsed:       4,
			Timestamp:     4,
			Coinbase:      gointerfaces.ConvertAddressToH160(common.HexToAddress("0x1")),
			Transactions:  make([][]byte, 0),
		})
		done <- true
	}()

	<-reverseDownloadCh

	statusCh <- core.ExecutionStatus{
		Hash:  common.HexToHash("7088846e2055286ec5a95ce9f79b5d8877d228313e19ff936b4ebdfae393e3c5"),
		Valid: false,
	}
	<-done

	require.NoError(err)
	require.Equal(reply.Status, Invalid)
	replyHash := gointerfaces.ConvertH256ToHash(reply.LatestValidHash)
	require.Equal(replyHash[:], head.Bytes())
}

func TestInvalidRequest(t *testing.T) {
	db := memdb.New()
	ctx := context.Background()

	tx, _ := db.BeginRw(ctx)

	require := require.New(t)
	head := common.HexToHash("0x1")
	rawdb.WriteHeadBlockHash(tx, head)
	rawdb.WriteHeaderNumber(tx, head, 50)

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
			BlockNumber:   2,
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
	head := common.HexToHash("0x1")
	rawdb.WriteHeadBlockHash(tx, head)
	rawdb.WriteHeaderNumber(tx, head, 50)

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
			BlockNumber:   2,
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
