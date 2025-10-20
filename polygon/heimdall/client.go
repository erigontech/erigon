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

package heimdall

import (
	"context"
)

//go:generate mockgen -typed=true -destination=./client_mock.go -package=heimdall . Client
type Client interface {
	FetchLatestSpan(ctx context.Context) (*Span, error)
	FetchSpan(ctx context.Context, spanID uint64) (*Span, error)
	FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error)

	FetchStatus(ctx context.Context) (*Status, error)

	FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error)

	FetchMilestone(ctx context.Context, number int64) (*Milestone, error)
	FetchMilestoneCount(ctx context.Context) (int64, error)
	FetchFirstMilestoneNum(ctx context.Context) (int64, error)

	// FetchNoAckMilestone fetches a bool value whether milestone corresponding to the given id failed in the Heimdall
	FetchNoAckMilestone(ctx context.Context, milestoneID string) error

	// FetchLastNoAckMilestone fetches the latest failed milestone id
	FetchLastNoAckMilestone(ctx context.Context) (string, error)

	// FetchMilestoneID fetches a bool value whether milestone corresponding to the given id is in process in Heimdall
	FetchMilestoneID(ctx context.Context, milestoneID string) error

	Close()
}
