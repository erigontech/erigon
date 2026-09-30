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

package integrity

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
)

var errChunk = errors.New("bad chunk")

// failFast decides where a check stops, not whether the run failed. Reporting and carrying on is
// what an operator asks for when they want the whole list, and it has to still come back as a
// failure — otherwise a run that found problems is indistinguishable from a clean one.
func TestProblemsReportCarriesOnButStillFails(t *testing.T) {
	var p problems

	require.NoError(t, p.report(false, errChunk), "reporting must let the caller continue")
	require.NoError(t, p.report(false, errChunk))

	err := p.verdict("SomeCheck")
	require.Error(t, err)
	require.Contains(t, err.Error(), "SomeCheck")
	require.Contains(t, err.Error(), "2")
}

func TestProblemsFailFastReturnsTheFirstError(t *testing.T) {
	var p problems

	require.ErrorIs(t, p.report(true, errChunk), errChunk)
	require.NoError(t, p.verdict("SomeCheck"), "a stopped run reports through its return, not the tally")
}

func TestProblemsSilentWhenNothingReported(t *testing.T) {
	var p problems
	require.NoError(t, p.verdict("SomeCheck"))
}

// Checks fan out over block ranges, so the tally is written from several goroutines at once.
func TestProblemsIsConcurrencySafe(t *testing.T) {
	var p problems
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { _ = p.report(false, errChunk) })
	}
	wg.Wait()
	require.Contains(t, p.verdict("SomeCheck").Error(), "50")
}

// The fan-out is where the two receipt checks meet, so it is the one place that has to turn a
// reported problem into the run's verdict.
func TestParallelChunkCheckFailsOnAReportedProblem(t *testing.T) {
	sc, err := NewSamplerCfg(1, 1.0)
	require.NoError(t, err)

	reportOne := func(_ context.Context, _, _ uint64, _ kv.TemporalRoDB, _ dbservices.FullBlockReader, failFast bool, p *problems) error {
		return p.report(failFast, errChunk)
	}

	t.Run("continuing past problems still fails the run", func(t *testing.T) {
		err := parallelChunkCheck(context.Background(), sc.NewSampler(), 1, 500, nil, nil, false, "TestCheck", reportOne)
		require.Error(t, err)
		require.Contains(t, err.Error(), "TestCheck")
	})

	t.Run("failFast surfaces the original error", func(t *testing.T) {
		err := parallelChunkCheck(context.Background(), sc.NewSampler(), 1, 500, nil, nil, true, "TestCheck", reportOne)
		require.ErrorIs(t, err, errChunk)
	})

	t.Run("a clean run stays clean", func(t *testing.T) {
		clean := func(_ context.Context, _, _ uint64, _ kv.TemporalRoDB, _ dbservices.FullBlockReader, _ bool, _ *problems) error {
			return nil
		}
		require.NoError(t, parallelChunkCheck(context.Background(), sc.NewSampler(), 1, 500, nil, nil, false, "TestCheck", clean))
	})
}

// Checks classify their failures with ErrIntegrity so callers can tell a bad datadir from a broken
// run. The verdict has to carry it too, or the same defect is classifiable under --failFast and
// plain without it.
func TestProblemsVerdictIsAnIntegrityError(t *testing.T) {
	var p problems
	require.NoError(t, p.report(false, errChunk))
	require.ErrorIs(t, p.verdict("SomeCheck"), ErrIntegrity)
}

// An operational failure part-way through — a read error, or the group context the first one
// cancels siblings with — must not erase the problems already found. That count is the whole
// reason to run with --failFast=false.
func TestParallelChunkCheckKeepsTheTallyWhenAChunkErrors(t *testing.T) {
	sc, err := NewSamplerCfg(1, 1.0)
	require.NoError(t, err)
	opErr := errors.New("stream blew up")

	reportThenFail := func(_ context.Context, from, _ uint64, _ kv.TemporalRoDB, _ dbservices.FullBlockReader, failFast bool, p *problems) error {
		if reportErr := p.report(failFast, errChunk); reportErr != nil {
			return reportErr
		}
		if from == 1 {
			return opErr
		}
		return nil
	}

	err = parallelChunkCheck(context.Background(), sc.NewSampler(), 1, 500, nil, nil, false, "TestCheck", reportThenFail)
	require.ErrorIs(t, err, opErr, "the operational failure must still surface")
	require.Contains(t, err.Error(), "TestCheck", "and the problems already found must not be lost")
}
