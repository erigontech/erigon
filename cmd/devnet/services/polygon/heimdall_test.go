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

package polygon

import (
	"context"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/polygon/heimdall"
)

func TestHeimdallServer(t *testing.T) {
	t.Skip()

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	client := heimdall.NewMockHeimdallClient(ctrl)

	events := []*heimdall.EventRecordWithTime{
		{
			EventRecord: heimdall.EventRecord{
				ID:      1,
				ChainID: "80002",
			},
			Time: time.Now(),
		},
		{
			EventRecord: heimdall.EventRecord{
				ID:      2,
				ChainID: "80002",
			},
			Time: time.Now(),
		},
	}
	client.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)

	span := &heimdall.Span{
		Id:         1,
		StartBlock: 1000,
		EndBlock:   2000,
		ChainID:    "80002",
	}
	client.EXPECT().FetchSpan(gomock.Any(), gomock.Any()).AnyTimes().Return(span, nil)

	checkpoint1 := &heimdall.Checkpoint{
		Fields: heimdall.WaypointFields{
			StartBlock: big.NewInt(1000),
			EndBlock:   big.NewInt(1999),
			ChainID:    "80002",
		},
	}
	client.EXPECT().FetchCheckpoint(gomock.Any(), gomock.Any()).AnyTimes().Return(checkpoint1, nil)
	client.EXPECT().FetchCheckpointCount(gomock.Any()).AnyTimes().Return(int64(1), nil)

	err := http.ListenAndServe(HeimdallURLDefault[7:], makeHeimdallRouter(ctx, client))
	require.Nil(t, err)
}
