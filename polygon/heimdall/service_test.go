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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestSpanProducerSelection(t *testing.T) {
	// do for span 0
	spanZero := &Span{}
	validatorSet := valset.NewValidatorSet(spanZero.Producers())
	accumPriorities := SpanBlockProducerSelection{
		SpanId:    SpanIdAt(0),
		Producers: validatorSet.Validators,
	}

	// then onwards for every new span need to do
	// 1. validatorSet.IncrementProposerPriority(sprintCountInSpan)
	// 2. validatorSet.UpdateWithChangeSet
	logger := testlog.Logger(t, log.LvlDebug)
	newSpan := &Span{}
	validatorSet = valset.GetUpdatedValidatorSet(validatorSet, newSpan.Producers(), logger)
	validatorSet.IncrementProposerPriority(1)
	accumPriorities = SpanBlockProducerSelection{
		SpanId:    SpanId(1),
		Producers: validatorSet.Validators,
	}

	fmt.Println(accumPriorities)

	// have a span producers tracker component that the heimdall service uses
	// it registers for receiving new span updates
	// upon changing from span X to span Y it performs UpdateWithChangeSet
	// and persists the new producer priorities in the DB
	// TODO implement this component
}

const spanTestDataDir = "testdata/amoy/spans"

func TestServiceFetchLatestSpans(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctrl := gomock.NewController(t)
	tempDir := t.TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	logger := testlog.Logger(t, log.LvlDebug)
	borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
	client := NewMockHeimdallClient(ctrl)
	client.EXPECT().
		FetchSpan(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id uint64) (*Span, error) {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/span_%d.json", spanTestDataDir, id))
			if err != nil {
				return nil, err
			}

			var span Span
			err = json.Unmarshal(bytes, &span)
			if err != nil {
				return nil, err
			}

			return &span, nil
		}).
		AnyTimes()
	client.EXPECT().
		FetchLatestSpan(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (*Span, error) {
			files, err := dir.ReadDir(spanTestDataDir)
			if err != nil {
				return nil, err
			}

			sort.Slice(files, func(i, j int) bool {
				r := regexp.MustCompile("span_([0-9]+).json")
				matchI := r.FindStringSubmatch(files[i].Name())
				matchJ := r.FindStringSubmatch(files[j].Name())
				spanIdI, err := strconv.ParseInt(matchI[1], 10, 64)
				if err != nil {
					panic(fmt.Errorf("could not parse span id from file name: %w", err))
				}
				spanIdJ, err := strconv.ParseInt(matchJ[1], 10, 64)
				if err != nil {
					panic(fmt.Errorf("could not parse span id from file name: %w", err))
				}
				return spanIdI < spanIdJ
			})

			bytes, err := os.ReadFile(fmt.Sprintf("%s/%s", spanTestDataDir, files[len(files)-1].Name()))
			if err != nil {
				return nil, err
			}

			var span Span
			err = json.Unmarshal(bytes, &span)
			if err != nil {
				return nil, err
			}

			return &span, nil
		}).
		AnyTimes()
	client.EXPECT().
		FetchCheckpointCount(gomock.Any()).
		Return(0, nil).
		AnyTimes()
	client.EXPECT().
		FetchMilestoneCount(gomock.Any()).
		Return(0, nil).
		AnyTimes()
	client.EXPECT().
		FetchFirstMilestoneNum(gomock.Any()).
		Return(1, nil).
		AnyTimes()
	store := NewMdbxServiceStore(logger, dataDir, tempDir)
	svc := NewService(borConfig, client, store, logger)
	go func() {
		err := svc.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			panic(fmt.Errorf("service err: %w", err))
		}
	}()

	spans, err := svc.FetchLatestSpans(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, spans[0].Id, SpanId(1548))
	require.Equal(t, spans[1].Id, SpanId(1549))
	require.Equal(t, spans[2].Id, SpanId(1550))
}
