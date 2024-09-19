package heimdall

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

type Reader struct {
	logger                    log.Logger
	store                     Store
	spanBlockProducersTracker *spanBlockProducersTracker
}

type ReaderConfig struct {
	CalculateSprintNumberFn CalculateSprintNumberFunc
	DataDir                 string
	Logger                  log.Logger
	RoTxLimit               int64
}

// AssembleReader creates and opens the MDBX store. For use cases where the store is only being read from. Must call Close.
func AssembleReader(ctx context.Context, config ReaderConfig) (*Reader, error) {
	store := NewMdbxStore(config.Logger, config.DataDir, config.RoTxLimit)

	err := store.Prepare(Ctx)
	if err != nil {
		return nil, err
	}

	return NewReader(config.CalculateSprintNumberFn, store, config.Logger), nil
}

func NewReader(calculateSprintNumber CalculateSprintNumberFunc, store Store, logger log.Logger) *Reader {
	return &Reader{
		logger:                    logger,
		store:                     store,
		spanBlockProducersTracker: newSpanBlockProducersTracker(logger, calculateSprintNumber, store.SpanBlockProducerSelections()),
	}
}

func (r *Reader) Span(ctx context.Context, id uint64) (*Span, bool, error) {
	return r.store.Spans().Entity(ctx, id)
}

func (r *Reader) CheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	entities, err := r.store.Checkpoints().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Checkpoint]), err
}

func (r *Reader) MilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	entities, err := r.store.Milestones().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Milestone]), err
}

func (r *Reader) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	return r.spanBlockProducersTracker.Producers(ctx, blockNum)
}

func (r *Reader) Close() {
	r.store.Close()
}

type RemoteReader struct {
	client  remote.HeimdallBackendClient
	logger  log.Logger
	version gointerfaces.Version
}

func NewRemoteReader(client remote.HeimdallBackendClient) *RemoteReader {
	return &RemoteReader{
		client:  client,
		logger:  log.New("remote_service", "heimdall"),
		version: gointerfaces.VersionFromProto(APIVersion),
	}
}

func (r *RemoteReader) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	reply, err := r.client.Producers(ctx, &remote.BorProducersRequest{BlockNum: blockNum})
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, nil
	}

	validators := reply.Validators
	proposer := reply.Proposer

	v := make([]*valset.Validator, len(validators))
	for i, validator := range validators {
		v[i] = decodeValidator(validator)
	}

	validatorSet := valset.ValidatorSet{
		Proposer:   decodeValidator(proposer),
		Validators: v,
	}

	return &validatorSet, nil
}

// Close implements bridge.ReaderService. It's a noop as there is no attached store.
func (r *RemoteReader) Close() {
	return
}

func (r *RemoteReader) EnsureVersionCompatibility() bool {
	versionReply, err := r.client.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		r.logger.Error("getting Version", "err", err)
		return false
	}
	if !gointerfaces.EnsureVersion(r.version, versionReply) {
		r.logger.Error("incompatible interface versions", "client", r.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	r.logger.Info("interfaces compatible", "client", r.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}

func decodeValidator(v *remote.Validator) *valset.Validator {
	return &valset.Validator{
		ID:               v.Id,
		Address:          gointerfaces.ConvertH160toAddress(v.Address),
		VotingPower:      v.VotingPower,
		ProposerPriority: v.ProposerPriority,
	}
}
