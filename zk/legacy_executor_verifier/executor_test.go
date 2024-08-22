package legacy_executor_verifier

import (
	"context"
	"errors"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockExecutorServiceClient struct {
	shouldError bool
}

func (m *mockExecutorServiceClient) ProcessBatch(ctx context.Context, in *executor.ProcessBatchRequest, opts ...grpc.CallOption) (*executor.ProcessBatchResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockExecutorServiceClient) ProcessBatchV2(ctx context.Context, in *executor.ProcessBatchRequestV2, opts ...grpc.CallOption) (*executor.ProcessBatchResponseV2, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockExecutorServiceClient) GetFlushStatus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*executor.GetFlushStatusResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockExecutorServiceClient) ProcessStatelessBatchV2(ctx context.Context, in *executor.ProcessStatelessBatchRequestV2, opts ...grpc.CallOption) (*executor.ProcessBatchResponseV2, error) {
	if m.shouldError {
		return nil, errors.New("mock error")
	}
	return &executor.ProcessBatchResponseV2{
		NewStateRoot: common.Hash{0}.Bytes(),
	}, nil
}

func TestExecutor_Verify(t *testing.T) {
	tests := []struct {
		name              string
		expectedStateRoot *common.Hash
		shouldError       bool
		wantErr           bool
	}{
		{"Success", &common.Hash{0}, false, false},
		{"gRPC Error", &common.Hash{0}, true, true},
	}

	for _, tt := range tests {
		func(tt struct {
			name              string
			expectedStateRoot *common.Hash
			shouldError       bool
			wantErr           bool
		}) {
			t.Run(tt.name, func(t *testing.T) {
				mockClient := &mockExecutorServiceClient{
					shouldError: tt.shouldError,
				}

				executor := &Executor{
					grpcUrl:    "",
					conn:       nil,
					connCancel: nil,
					client:     mockClient,
					semaphore:  make(chan struct{}),
				}

				payload := &Payload{
					Witness:           []byte{0, 1},
					DataStream:        []byte{2, 3},
					Coinbase:          "0x000000000",
					OldAccInputHash:   []byte{4, 5},
					L1InfoRoot:        []byte{6, 7},
					TimestampLimit:    100,
					ForcedBlockhashL1: []byte{8, 9},
					ContextId:         "cdk-erigon-test",
				}

				_, _, executorErr, generalErr := executor.Verify(payload, &VerifierRequest{StateRoot: *tt.expectedStateRoot}, common.Hash{})
				if (executorErr != nil || generalErr != nil) != tt.wantErr {
					t.Errorf("Executor.Verify() executorErr = %v, generalErr = %v, wantErr %v", executorErr, generalErr, tt.wantErr)
				}
			})
		}(tt)
	}
}
