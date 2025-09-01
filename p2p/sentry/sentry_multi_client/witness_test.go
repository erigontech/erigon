package sentry_multi_client

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/stateless"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

func addTestWitnessData(db kv.TemporalRwDB, hash common.Hash, witnessData []byte, blockNumber uint64) error {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	header := &types.Header{
		Number: big.NewInt(int64(blockNumber)),
	}

	headerBytes, err := rlp.EncodeToBytes(header)
	if err != nil {
		return err
	}

	blockNumberBytes := dbutils.EncodeBlockNumber(blockNumber)
	err = tx.Put(kv.HeaderNumber, hash.Bytes(), blockNumberBytes)
	if err != nil {
		return err
	}

	headerKey := dbutils.HeaderKey(blockNumber, hash)
	err = tx.Put(kv.Headers, headerKey, headerBytes)
	if err != nil {
		return err
	}

	witnessKey := dbutils.HeaderKey(blockNumber, hash)
	err = tx.Put(kv.BorWitnesses, witnessKey, witnessData)
	if err != nil {
		return err
	}

	sizeBytes := dbutils.EncodeBlockNumber(uint64(len(witnessData)))
	err = tx.Put(kv.BorWitnessSizes, witnessKey, sizeBytes)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func createTestWitness(t *testing.T, header *types.Header) *stateless.Witness {
	t.Helper()

	witness, err := stateless.NewWitness(header, nil)
	require.NoError(t, err)

	testState := map[string]struct{}{
		"test_state_node_1": {},
		"test_state_node_2": {},
	}
	witness.AddState(testState)
	witness.AddCode([]byte("test_code_data"))

	return witness
}

func createTestMultiClient(t *testing.T) (*MultiClient, kv.TemporalRwDB) {
	baseDB := memdb.NewStateDB(t.TempDir())
	t.Cleanup(baseDB.Close)

	dirs, logger := datadir.New(t.TempDir()), log.New()
	salt, err := dbstate.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(t, err)
	agg, err := dbstate.NewAggregator2(context.Background(), dirs, 16, salt, baseDB, logger)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	tdb, err := temporal.New(baseDB, agg)
	require.NoError(t, err)

	witnessBuffer := stagedsync.NewWitnessBuffer()

	return &MultiClient{
		db:            tdb,
		WitnessBuffer: witnessBuffer,
		logger:        logger,
		blockReader:   freezeblocks.NewBlockReader(nil, nil),
	}, tdb
}

func TestGetBlockWitnessesFunction(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockSentryClient := direct.NewMockSentryClient(ctrl)
	multiClient, testDB := createTestMultiClient(t)

	t.Run("Invalid RLP", func(t *testing.T) {
		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   []byte{0xFF, 0xFF, 0xFF}, // Invalid RLP
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x01, 0x02, 0x03}),
		}

		err := multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decoding GetWitnessPacket")
	})

	t.Run("Valid RLP with Database Data Returns Correct Response", func(t *testing.T) {
		testBlockHash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		testWitnessData := []byte("test_witness")
		blockNumber := uint64(100)
		err := addTestWitnessData(testDB, testBlockHash, testWitnessData, blockNumber)
		require.NoError(t, err)

		req := wit.GetWitnessPacket{
			RequestId: 123,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 0,
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x01, 0x02, 0x03}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
				require.Equal(t, proto_sentry.MessageId_BLOCK_WITNESS_W0, request.Data.Id)

				var response wit.WitnessPacketRLPPacket
				err := rlp.DecodeBytes(request.Data.Data, &response)
				require.NoError(t, err)
				require.Equal(t, uint64(123), response.RequestId)
				require.Len(t, response.WitnessPacketResponse, 1)

				pageResp := response.WitnessPacketResponse[0]
				require.Equal(t, testBlockHash, pageResp.Hash)
				require.Equal(t, uint64(0), pageResp.Page)
				require.Equal(t, uint64(1), pageResp.TotalPages)
				require.Equal(t, testWitnessData, pageResp.Data)

				return &proto_sentry.SentPeers{}, nil
			},
		).Times(1)

		err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)
	})
}

func TestNewWitnessFunction(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockSentryClient := direct.NewMockSentryClient(ctrl)
	multiClient, _ := createTestMultiClient(t)

	t.Run("Invalid RLP", func(t *testing.T) {
		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_NEW_WITNESS_W0,
			Data:   []byte{0xFF, 0xFF, 0xFF}, // Invalid RLP
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x01, 0x02, 0x03}),
		}

		err := multiClient.newWitness(ctx, inboundMsg, mockSentryClient)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decoding")
	})

	t.Run("Valid RLP Stores Data in Buffer", func(t *testing.T) {
		testHeader := &types.Header{
			Number:     big.NewInt(200),
			ParentHash: common.HexToHash("0xparent"),
			Root:       common.HexToHash("0xroot"),
		}
		witness := createTestWitness(t, testHeader)
		expectedBlockHash := testHeader.Hash()

		newWitnessPacket := wit.NewWitnessPacket{
			Witness: witness,
		}

		packetData, err := rlp.EncodeToBytes(&newWitnessPacket)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_NEW_WITNESS_W0,
			Data:   packetData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x01, 0x02, 0x03}),
		}

		// Store the initial buffer state
		initialBufferLength := len(multiClient.WitnessBuffer.DrainWitnesses())
		// Restore the buffer since DrainWitnesses clears it
		if initialBufferLength > 0 {
			// This shouldn't happen in a fresh test, but just in case
			t.Fatal("Buffer should be empty at start of test")
		}

		err = multiClient.newWitness(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)

		// Check that witness data was added to the buffer
		witnesses := multiClient.WitnessBuffer.DrainWitnesses()
		require.Len(t, witnesses, 1, "Should have exactly one witness in buffer")

		storedWitness := witnesses[0]
		require.Equal(t, uint64(200), storedWitness.BlockNumber, "Block number should match")
		require.Equal(t, expectedBlockHash, storedWitness.BlockHash, "Block hash should match")
		require.Greater(t, len(storedWitness.Data), 0, "Witness data should not be empty")
	})
}

func TestWitnessFunctionsThroughMessageHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockSentryClient := direct.NewMockSentryClient(ctrl)
	multiClient, testDB := createTestMultiClient(t)

	t.Run("Message Handler Routes to getBlockWitnesses with Data", func(t *testing.T) {
		testBlockHash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		testWitnessData := []byte("test witness data for message handler test")

		err := addTestWitnessData(testDB, testBlockHash, testWitnessData, 100)
		require.NoError(t, err)

		req := wit.GetWitnessPacket{
			RequestId: 123,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 0,
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x01, 0x02, 0x03}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).Return(&proto_sentry.SentPeers{}, nil).Times(1)

		err = multiClient.handleInboundMessage(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err) // Should succeed with proper data
	})

	t.Run("Message Handler Routes to newWitness", func(t *testing.T) {
		testHeader := &types.Header{
			Number:     big.NewInt(200),
			ParentHash: common.HexToHash("0xparent456"),
			Root:       common.HexToHash("0xroot456"),
		}
		witness := createTestWitness(t, testHeader)
		expectedBlockHash := testHeader.Hash()

		newWitnessPacket := wit.NewWitnessPacket{
			Witness: witness,
		}

		packetData, err := rlp.EncodeToBytes(&newWitnessPacket)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_NEW_WITNESS_W0,
			Data:   packetData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x01, 0x02, 0x03}),
		}

		// Clear any existing data in buffer
		multiClient.WitnessBuffer.DrainWitnesses()

		err = multiClient.handleInboundMessage(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)

		// Check that witness data was added to the buffer
		witnesses := multiClient.WitnessBuffer.DrainWitnesses()
		require.Len(t, witnesses, 1, "Should have exactly one witness in buffer")

		storedWitness := witnesses[0]
		require.Equal(t, uint64(200), storedWitness.BlockNumber, "Block number should match")
		require.Equal(t, expectedBlockHash, storedWitness.BlockHash, "Block hash should match")
		require.Greater(t, len(storedWitness.Data), 0, "Witness data should not be empty")
	})
}

// Test pagination with large witness data that spans multiple pages
func TestWitnessPagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockSentryClient := direct.NewMockSentryClient(ctrl)
	multiClient, testDB := createTestMultiClient(t)

	testBlockHash := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	// Create witness data that's larger than 2 pages but smaller than 3 pages
	// This will test: page 0 (full), page 1 (full), page 2 (partial)
	pageSize := wit.PageSize                          // 15 MB
	largeWitnessData := make([]byte, pageSize*2+1000) // ~30MB + 1KB

	// Fill with test pattern to verify data integrity
	for i := range largeWitnessData {
		largeWitnessData[i] = byte(i % 256)
	}

	err := addTestWitnessData(testDB, testBlockHash, largeWitnessData, 100)
	require.NoError(t, err)

	t.Run("Request Page 0 - First Page", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 456,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 0,
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x04, 0x05, 0x06}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
				var response wit.WitnessPacketRLPPacket
				err := rlp.DecodeBytes(request.Data.Data, &response)
				require.NoError(t, err)

				require.Equal(t, uint64(456), response.RequestId)
				require.Len(t, response.WitnessPacketResponse, 1)

				pageResp := response.WitnessPacketResponse[0]
				require.Equal(t, testBlockHash, pageResp.Hash)
				require.Equal(t, uint64(0), pageResp.Page)
				require.Equal(t, uint64(3), pageResp.TotalPages) // Should be 3 pages total
				require.Equal(t, pageSize, len(pageResp.Data))   // Full page size

				expectedFirstPage := largeWitnessData[:pageSize]
				require.Equal(t, expectedFirstPage, pageResp.Data)

				return &proto_sentry.SentPeers{}, nil
			},
		).Times(1)

		err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)
	})

	t.Run("Request Page 1 - Middle Page", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 457,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 1,
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x04, 0x05, 0x06}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
				var response wit.WitnessPacketRLPPacket
				err := rlp.DecodeBytes(request.Data.Data, &response)
				require.NoError(t, err)

				pageResp := response.WitnessPacketResponse[0]
				require.Equal(t, testBlockHash, pageResp.Hash)
				require.Equal(t, uint64(1), pageResp.Page)
				require.Equal(t, uint64(3), pageResp.TotalPages)
				require.Equal(t, pageSize, len(pageResp.Data)) // Full page size

				expectedSecondPage := largeWitnessData[pageSize : pageSize*2]
				require.Equal(t, expectedSecondPage, pageResp.Data)

				return &proto_sentry.SentPeers{}, nil
			},
		).Times(1)

		err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)
	})

	t.Run("Request Page 2 - Last Partial Page", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 458,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 2,
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x04, 0x05, 0x06}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
				var response wit.WitnessPacketRLPPacket
				err := rlp.DecodeBytes(request.Data.Data, &response)
				require.NoError(t, err)

				pageResp := response.WitnessPacketResponse[0]
				require.Equal(t, testBlockHash, pageResp.Hash)
				require.Equal(t, uint64(2), pageResp.Page)
				require.Equal(t, uint64(3), pageResp.TotalPages)
				require.Equal(t, 1000, len(pageResp.Data)) // Partial page size (1000 bytes)

				expectedThirdPage := largeWitnessData[pageSize*2:]
				require.Equal(t, expectedThirdPage, pageResp.Data)

				return &proto_sentry.SentPeers{}, nil
			},
		).Times(1)

		err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)
	})

	t.Run("Request Multiple Pages in Single Request", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 459,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 0,
				},
				{
					Hash: testBlockHash,
					Page: 2,
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x04, 0x05, 0x06}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
				var response wit.WitnessPacketRLPPacket
				err := rlp.DecodeBytes(request.Data.Data, &response)
				require.NoError(t, err)

				require.Equal(t, uint64(459), response.RequestId)
				require.Len(t, response.WitnessPacketResponse, 2) // Should have 2 pages

				// Check page 0
				page0 := response.WitnessPacketResponse[0]
				require.Equal(t, testBlockHash, page0.Hash)
				require.Equal(t, uint64(0), page0.Page)
				require.Equal(t, uint64(3), page0.TotalPages)
				require.Equal(t, pageSize, len(page0.Data))

				// Check page 2
				page2 := response.WitnessPacketResponse[1]
				require.Equal(t, testBlockHash, page2.Hash)
				require.Equal(t, uint64(2), page2.Page)
				require.Equal(t, uint64(3), page2.TotalPages)
				require.Equal(t, 1000, len(page2.Data))

				return &proto_sentry.SentPeers{}, nil
			},
		).Times(1)

		err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)
	})

	t.Run("Request Invalid Page Number", func(t *testing.T) {
		req := wit.GetWitnessPacket{
			RequestId: 460,
			GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
				{
					Hash: testBlockHash,
					Page: 10, // Invalid page (only 3 pages exist)
				},
			}},
		}

		reqData, err := rlp.EncodeToBytes(&req)
		require.NoError(t, err)

		inboundMsg := &proto_sentry.InboundMessage{
			Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
			Data:   reqData,
			PeerId: gointerfaces.ConvertHashToH512([64]byte{0x04, 0x05, 0x06}),
		}

		mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
				var response wit.WitnessPacketRLPPacket
				err := rlp.DecodeBytes(request.Data.Data, &response)
				require.NoError(t, err)

				pageResp := response.WitnessPacketResponse[0]
				require.Equal(t, testBlockHash, pageResp.Hash)
				require.Equal(t, uint64(10), pageResp.Page)
				require.Equal(t, uint64(3), pageResp.TotalPages)
				require.Empty(t, pageResp.Data) // Should be empty for invalid page

				return &proto_sentry.SentPeers{}, nil
			},
		).Times(1)

		err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
		require.NoError(t, err)
	})
}

// Test edge case: witness that's exactly one page size
func TestWitnessExactPageSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockSentryClient := direct.NewMockSentryClient(ctrl)
	multiClient, testDB := createTestMultiClient(t)

	testBlockHash := common.HexToHash("0xedgecase1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	pageSize := wit.PageSize
	exactPageSizeData := make([]byte, pageSize) // Exactly one page

	// Fill with test pattern
	for i := range exactPageSizeData {
		exactPageSizeData[i] = byte(i % 256)
	}

	err := addTestWitnessData(testDB, testBlockHash, exactPageSizeData, 100)
	require.NoError(t, err)

	req := wit.GetWitnessPacket{
		RequestId: 999,
		GetWitnessRequest: &wit.GetWitnessRequest{WitnessPages: []wit.WitnessPageRequest{
			{
				Hash: testBlockHash,
				Page: 0,
			},
		}},
	}

	reqData, err := rlp.EncodeToBytes(&req)
	require.NoError(t, err)

	inboundMsg := &proto_sentry.InboundMessage{
		Id:     proto_sentry.MessageId_GET_BLOCK_WITNESS_W0,
		Data:   reqData,
		PeerId: gointerfaces.ConvertHashToH512([64]byte{0x99, 0x99, 0x99}),
	}

	mockSentryClient.EXPECT().SendMessageById(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *proto_sentry.SendMessageByIdRequest, opts ...grpc.CallOption) (*proto_sentry.SentPeers, error) {
			var response wit.WitnessPacketRLPPacket
			err := rlp.DecodeBytes(request.Data.Data, &response)
			require.NoError(t, err)

			require.Equal(t, uint64(999), response.RequestId)
			require.Len(t, response.WitnessPacketResponse, 1)

			pageResp := response.WitnessPacketResponse[0]
			require.Equal(t, testBlockHash, pageResp.Hash)
			require.Equal(t, uint64(0), pageResp.Page)
			require.Equal(t, uint64(1), pageResp.TotalPages) // Should be exactly 1 page
			require.Equal(t, pageSize, len(pageResp.Data))   // Full page size
			require.Equal(t, exactPageSizeData, pageResp.Data)

			return &proto_sentry.SentPeers{}, nil
		},
	).Times(1)

	err = multiClient.getBlockWitnesses(ctx, inboundMsg, mockSentryClient)
	require.NoError(t, err)
}
