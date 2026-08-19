// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execution_client

import (
	"context"
	"errors"
	"math/big"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// ErrForkChoiceUpdateTimeout reports that the execution layer did not answer a forkchoice update
// in time. The update is not refused by it - the execution layer may still apply it - and no
// payload id came back, so a caller that needed one has to treat this as a failure rather than as
// an empty success.
var ErrForkChoiceUpdateTimeout = errors.New("forkchoice update timed out")

// ErrForkChoiceUpdateNoPayloadID reports that an attribute-bearing update did not start a payload build.
var ErrForkChoiceUpdateNoPayloadID = errors.New("forkchoice update returned no payload ID")

// legacyGrpcDeadlineMessage is what a deadline used to be recognised by. Kept as a last resort for
// a transport that reports one without a status code or a wrapped context error.
const legacyGrpcDeadlineMessage = "rpc error: code = DeadlineExceeded desc = context deadline exceeded"

// isDeadlineExceeded reports whether err is the execution layer running out of time, by the
// context, by the gRPC status, or by the message a transport that carries neither would produce.
func isDeadlineExceeded(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	return err.Error() == legacyGrpcDeadlineMessage
}

// ExecutionEngine is used only for syncing up very close to chain tip and to stay in sync.
// It pretty much mimics engine API.

//go:generate mockgen -typed=true -source=./interface.go -destination=./execution_engine_mock.go -package=execution_client . ExecutionEngine
type ExecutionEngine interface {
	NewPayload(ctx context.Context, payload *cltypes.Eth1Block, beaconParentRoot *common.Hash, versionedHashes []common.Hash, executionRequestsList []hexutil.Bytes) (PayloadStatus, error)
	ForkChoiceUpdate(ctx context.Context, finalized, safe, head common.Hash, attributes *engine_types.PayloadAttributes, version clparams.StateVersion) ([]byte, error)
	SupportInsertion() bool
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	InsertBlock(ctx context.Context, block *types.Block) error
	CurrentHeader(ctx context.Context) (*types.Header, error)
	IsCanonicalHash(ctx context.Context, hash common.Hash) (bool, error)
	Ready(ctx context.Context) (bool, error)
	// Range methods
	GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error)
	GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error)
	HasBlock(ctx context.Context, hash common.Hash) (bool, error)
	// Snapshots
	FrozenBlocks(ctx context.Context) uint64
	HasGapInSnapshots(ctx context.Context) bool
	// Block production
	GetAssembledBlock(ctx context.Context, id []byte, version clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error)

	// Blobs
	GetBlobs(ctx context.Context, versionedHashes []common.Hash, version clparams.StateVersion) (blobs [][]byte, proofs [][][]byte, err error)
	// Client identification
	GetClientVersionV1(ctx context.Context, callerVersion *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error)
}

const (
	// Leave room for the default one-second FCU timeout to settle without holding consensus work
	// for a substantial part of the slot.
	forkChoiceUpdateRetryWindow = 2 * time.Second
	forkChoiceUpdateRetryDelay  = 100 * time.Millisecond
)

// RetryForkChoiceUpdate gives an earlier asynchronous update a bounded window to settle before
// resending the canonical head. If the window closes first, it returns the last contention error.
func RetryForkChoiceUpdate(
	ctx context.Context,
	engine ExecutionEngine,
	finalized, safe, head common.Hash,
	version clparams.StateVersion,
) ([]byte, error) {
	return retryForkChoiceUpdate(
		ctx, engine, finalized, safe, head, version,
		forkChoiceUpdateRetryWindow, forkChoiceUpdateRetryDelay,
	)
}

func retryForkChoiceUpdate(
	ctx context.Context,
	engine ExecutionEngine,
	finalized, safe, head common.Hash,
	version clparams.StateVersion,
	retryWindow, retryDelay time.Duration,
) ([]byte, error) {
	retryCtx, cancel := context.WithTimeout(ctx, retryWindow)
	defer cancel()

	var lastErr error
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if retryCtx.Err() != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, retryCtx.Err()
		}
		payloadID, err := engine.ForkChoiceUpdate(retryCtx, finalized, safe, head, nil, version)
		if err == nil || !isForkChoiceUpdateContention(err) {
			return payloadID, err
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if retryCtx.Err() != nil {
			return nil, lastErr
		}

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-retryCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, lastErr
		case <-timer.C:
		}
	}
}

func isForkChoiceUpdateContention(err error) bool {
	return errors.Is(err, ErrForkChoiceBusy) ||
		errors.Is(err, ErrForkChoiceUpdateTimeout) ||
		errors.Is(err, context.DeadlineExceeded)
}
