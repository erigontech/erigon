// Copyright 2026 The Erigon Authors
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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

// ErrPayloadBuildHeadMismatch reports that the execution layer has not adopted the parent on which
// preparation wants to build. A later canonical forkchoice update may make the request valid.
var ErrPayloadBuildHeadMismatch = errors.New("execution head does not match payload parent")

// PayloadBuilder starts a build through the local execution module without changing fork choice.
// An implementation backed only by a remote Engine API returns ErrNotSupported.
type PayloadBuilder interface {
	StartPayloadBuild(ctx context.Context, head common.Hash, attributes *engine_types.PayloadAttributes) ([]byte, error)
}

func startPayloadBuild(
	ctx context.Context,
	chainRW chainreader.ChainReaderWriterEth1,
	head common.Hash,
	attributes *engine_types.PayloadAttributes,
) (uint64, error) {
	executionHead, _, _, err := chainRW.GetForkChoice(ctx)
	if err != nil {
		return 0, err
	}
	if executionHead != head {
		return 0, fmt.Errorf("%w: have %x, want %x", ErrPayloadBuildHeadMismatch, executionHead, head)
	}
	return chainRW.AssembleBlock(ctx, head, attributes)
}
