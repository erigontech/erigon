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

package transactions

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native" // registers callTracer
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func assembleWithLogConfig(t *testing.T, cfg *logger.LogConfig, tracerName *string) error {
	t.Helper()
	_, _, cancel, err := AssembleTracer(
		t.Context(),
		&tracersConfig.TraceConfig{LogConfig: cfg, Tracer: tracerName},
		common.Hash{}, nil, common.Hash{}, 0,
		jsonstream.New(io.Discard),
		time.Second,
	)
	cancel()
	return err
}

// execution-apis gives the opcode logger's limit a minimum of 0, and a negative
// one would suppress every step, so it must be refused rather than served as an
// empty trace.
func TestAssembleTracerRejectsNegativeLimit(t *testing.T) {
	err := assembleWithLogConfig(t, &logger.LogConfig{Limit: -1}, nil)

	var invalidParams *rpc.InvalidParamsError
	require.ErrorAs(t, err, &invalidParams)
}

func TestAssembleTracerAcceptsNonNegativeLimit(t *testing.T) {
	for _, limit := range []int{0, 1, 1000} {
		require.NoError(t, assembleWithLogConfig(t, &logger.LogConfig{Limit: limit}, nil))
	}
}

// The limit belongs to the opcode logger, and execution-apis says a named tracer
// ignores it, so it must not turn into an error there.
func TestAssembleTracerIgnoresLimitForNamedTracer(t *testing.T) {
	callTracer := "callTracer"
	require.NoError(t, assembleWithLogConfig(t, &logger.LogConfig{Limit: -1}, &callTracer))
}
