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
	"math/big"
	"time"

	"github.com/erigontech/erigon/execution/builder/buildercfg"
)

type IdleClient struct {
	cfg buildercfg.MiningConfig
}

func NewIdleClient(cfg buildercfg.MiningConfig) Client {
	return &IdleClient{cfg: cfg}
}

func (c *IdleClient) FetchLatestSpan(ctx context.Context) (*Span, error) {
	return &Span{
		StartBlock: 0,
		EndBlock:   255,
		ValidatorSet: ValidatorSet{
			Validators: []*Validator{
				{
					ID:          0,
					Address:     c.cfg.Etherbase,
					VotingPower: 1,
				},
			},
		},
		SelectedProducers: []Validator{
			{
				ID:          0,
				Address:     c.cfg.Etherbase,
				VotingPower: 1,
			},
		},
	}, nil
}

func (c *IdleClient) FetchSpan(ctx context.Context, spanID uint64) (*Span, error) {
	return &Span{
		Id:         SpanId(spanID),
		StartBlock: 0,
		EndBlock:   255,
		ValidatorSet: ValidatorSet{
			Validators: []*Validator{
				{
					ID:          0,
					Address:     c.cfg.Etherbase,
					VotingPower: 1,
				},
			},
		},
		SelectedProducers: []Validator{
			{
				ID:          0,
				Address:     c.cfg.Etherbase,
				VotingPower: 1,
			},
		},
	}, nil
}

func (c *IdleClient) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error) {
	return nil, nil
}

func (c *IdleClient) FetchStatus(ctx context.Context) (*Status, error) {
	return &Status{
		LatestBlockTime: time.Now().Format(time.RFC3339),
		CatchingUp:      false,
	}, nil
}

func (c *IdleClient) FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error) {
	return nil, nil
}

func (c *IdleClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, nil
}

func (c *IdleClient) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	return nil, nil
}

func (c *IdleClient) FetchMilestone(ctx context.Context, number int64) (*Milestone, error) {
	return &Milestone{
		Id: MilestoneId(number),
		Fields: WaypointFields{
			StartBlock: big.NewInt(0),
			EndBlock:   big.NewInt(0),
		},
	}, nil
}

func (c *IdleClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	return 0, nil
}

func (c *IdleClient) FetchFirstMilestoneNum(ctx context.Context) (int64, error) {
	return 0, nil
}

func (c *IdleClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return nil
}

func (c *IdleClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	return "", nil
}

func (c *IdleClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return nil
}

func (c *IdleClient) Close() {
}
