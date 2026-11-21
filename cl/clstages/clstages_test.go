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

package clstages_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clstages"
	"github.com/erigontech/erigon/common/log/v3"
)

// A panic in a stage's ActionFunc must be recovered and surfaced as a stage
// error, never propagate out of the stage goroutine and terminate the process.
func TestStartWithStageRecoversFromActionFuncPanic(t *testing.T) {
	var transitionErr error
	graph := &clstages.StageGraph[any, any]{
		ArgsFunc: func(ctx context.Context, cfg any) any { return nil },
		Stages: map[string]clstages.Stage[any, any]{
			"boom": {
				Description: "panics on purpose",
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg any, args any) error {
					panic("kaboom")
				},
				TransitionFunc: func(cfg any, args any, err error) string {
					transitionErr = err
					return "terminal"
				},
			},
		},
	}

	err := graph.StartWithStage(context.Background(), "boom", log.Root(), nil)

	require.ErrorContains(t, err, "unknown stage: terminal")
	require.Error(t, transitionErr)
	require.ErrorContains(t, transitionErr, "kaboom")
}
