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

package sync

import (
	"context"

	"github.com/erigontech/erigon/polygon/heimdall"
)

//go:generate mockgen -typed=true -source=./waypoint_reader.go -destination=./waypoint_reader_mock.go -package=sync
type waypointReader interface {
	CheckpointsFromBlock(ctx context.Context, startBlock uint64) ([]*heimdall.Checkpoint, error)
	MilestonesFromBlock(ctx context.Context, startBlock uint64) ([]*heimdall.Milestone, error)
}
