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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/turbo/testlog"
)

const spanTestDataDir = "testdata/amoy/spans"
const proposerSequenceTestDataDir = "testdata/amoy/getSnapshotProposerSequence"

func TestService(t *testing.T) {
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

	t.Run("FetchLatestSpans", func(t *testing.T) {
		spans, err := svc.FetchLatestSpans(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, spans[0].Id, SpanId(1548))
		require.Equal(t, spans[1].Id, SpanId(1549))
		require.Equal(t, spans[2].Id, SpanId(1550))
	})

	t.Run("Producers", func(t *testing.T) {
		// span 0
		producersTest(ctx, t, svc, 1)   // start
		producersTest(ctx, t, svc, 255) // end
		// span 167
		producersTest(ctx, t, svc, 1062656) // start
		producersTest(ctx, t, svc, 1069055) // end
		// span 168 - first span that has changes to selected producers
		producersTest(ctx, t, svc, 1069056) // start
		producersTest(ctx, t, svc, 1072256) // middle
		producersTest(ctx, t, svc, 1075455) // end
		// span 169
		producersTest(ctx, t, svc, 1075456) // start
		producersTest(ctx, t, svc, 1081855) // end
		// span 182 - second span that has changes to selected producers
		producersTest(ctx, t, svc, 1158656) // start
		producersTest(ctx, t, svc, 1165055) // end
		// span 1279
		producersTest(ctx, t, svc, 8179456) // start
		producersTest(ctx, t, svc, 8185855) // end
		// span 1280 - span where we discovered the need for this API
		producersTest(ctx, t, svc, 8185856) // start
		producersTest(ctx, t, svc, 8187309) // middle where we discovered error
		producersTest(ctx, t, svc, 8192255) // end
	})
}

func producersTest(ctx context.Context, t *testing.T, svc Service, blockNum uint64) {
	t.Run(fmt.Sprintf("blockNum_%d", blockNum), func(t *testing.T) {
		b, err := os.ReadFile(fmt.Sprintf("%s/blockNum_%d.json", proposerSequenceTestDataDir, blockNum))
		require.NoError(t, err)
		var proposerSequenceResponse getSnapshotProposerSequenceResponse
		err = json.Unmarshal(b, &proposerSequenceResponse)
		require.NoError(t, err)
		want := proposerSequenceResponse.Result

		producers, err := svc.Producers(ctx, blockNum)
		require.NoError(t, err)

		errInfoMsgArgs := []interface{}{"want: %v\nhave: %v\n", want, producers}
		require.Equal(t, len(want.Signers), len(producers.Validators), errInfoMsgArgs...)
		for _, signer := range want.Signers {
			_, producer := producers.GetByAddress(signer.Signer)
			producerDifficulty, err := producers.Difficulty(producer.Address)
			require.NoError(t, err)
			require.Equal(t, signer.Difficulty, producerDifficulty, errInfoMsgArgs...)
		}
	})
}

// getSnapshotProposerSequenceResponse is reflecting the result from BOR API bor_getSnapshotProposerSequence for testing
type getSnapshotProposerSequenceResponse struct {
	Result proposerSequenceResult `json:"result"`
}

type proposerSequenceResult struct {
	Signers []difficultiesKV
}

type difficultiesKV struct {
	Signer     common.Address
	Difficulty uint64
}
