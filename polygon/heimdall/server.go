package heimdall

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

type spanProducersReader interface {
	Producers(ctx context.Context, blockNum uint64) (*ValidatorSet, error)
}

var APIVersion = &typesproto.VersionReply{Major: 1, Minor: 0, Patch: 0}

type BackendServer struct {
	remoteproto.UnimplementedHeimdallBackendServer // must be embedded to have forward compatible implementations.

	ctx                 context.Context
	spanProducersReader spanProducersReader
}

func NewBackendServer(ctx context.Context, spanProducersReader spanProducersReader) *BackendServer {
	return &BackendServer{
		ctx:                 ctx,
		spanProducersReader: spanProducersReader,
	}
}

func (b *BackendServer) Version(ctx context.Context, in *emptypb.Empty) (*typesproto.VersionReply, error) {
	return APIVersion, nil
}

func (b *BackendServer) Producers(ctx context.Context, in *remoteproto.BorProducersRequest) (*remoteproto.BorProducersResponse, error) {
	validatorSet, err := b.spanProducersReader.Producers(ctx, in.BlockNum)
	if err != nil {
		return nil, err
	}

	validators := make([]*remoteproto.Validator, len(validatorSet.Validators))
	for i, v := range validatorSet.Validators {
		validators[i] = encodeValidator(v)
	}

	return &remoteproto.BorProducersResponse{
		Proposer:   encodeValidator(validatorSet.Proposer),
		Validators: validators,
	}, nil
}

func encodeValidator(v *Validator) *remoteproto.Validator {
	return &remoteproto.Validator{
		Id:               v.ID,
		Address:          gointerfaces.ConvertAddressToH160(v.Address),
		VotingPower:      v.VotingPower,
		ProposerPriority: v.ProposerPriority,
	}
}
