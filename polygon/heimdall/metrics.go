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
	"time"

	"github.com/erigontech/erigon/erigon-lib/metrics"
)

type (
	requestTypeKey struct{}
	requestType    string

	meter struct {
		request map[bool]metrics.Gauge
		timer   metrics.Summary
	}
)

const (
	stateSyncRequest          requestType = "state-sync"
	spanRequest               requestType = "span"
	checkpointRequest         requestType = "checkpoint"
	checkpointCountRequest    requestType = "checkpoint-count"
	checkpointListRequest     requestType = "checkpoint-list"
	milestoneRequest          requestType = "milestone"
	milestoneCountRequest     requestType = "milestone-count"
	milestoneNoAckRequest     requestType = "milestone-no-ack"
	milestoneLastNoAckRequest requestType = "milestone-last-no-ack"
	milestoneIDRequest        requestType = "milestone-id"
)

func withRequestType(ctx context.Context, reqType requestType) context.Context {
	return context.WithValue(ctx, requestTypeKey{}, reqType)
}

func getRequestType(ctx context.Context) (requestType, bool) {
	reqType, ok := ctx.Value(requestTypeKey{}).(requestType)
	return reqType, ok
}

var (
	requestMeters = map[requestType]meter{
		stateSyncRequest: {
			request: map[bool]metrics.Gauge{
				true:  metrics.GetOrCreateGauge("client_requests_statesync_valid"),
				false: metrics.GetOrCreateGauge("client_requests_statesync_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_statesync_duration"),
		},
		spanRequest: {
			request: map[bool]metrics.Gauge{
				true:  metrics.GetOrCreateGauge("client_requests_span_valid"),
				false: metrics.GetOrCreateGauge("client_requests_span_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_span_duration"),
		},
		checkpointRequest: {
			request: map[bool]metrics.Gauge{
				true:  metrics.GetOrCreateGauge("client_requests_checkpoint_valid"),
				false: metrics.GetOrCreateGauge("client_requests_checkpoint_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_checkpoint_duration"),
		},
		checkpointCountRequest: {
			request: map[bool]metrics.Gauge{
				true:  metrics.GetOrCreateGauge("client_requests_checkpointcount_valid"),
				false: metrics.GetOrCreateGauge("client_requests_checkpointcount_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_checkpointcount_duration"),
		},
	}

	waypointCheckpointLength = metrics.NewGauge(`waypoint_length{type="checkpoint"}`)
	waypointMilestoneLength  = metrics.NewGauge(`waypoint_length{type="milestone"}`)
)

func sendMetrics(ctx context.Context, start time.Time, isSuccessful bool) {
	reqType, ok := getRequestType(ctx)
	if !ok {
		return
	}

	meters, ok := requestMeters[reqType]
	if !ok {
		return
	}

	meters.request[isSuccessful].Set(1)
	meters.timer.ObserveDuration(start)
}

func UpdateObservedWaypointCheckpointLength(length uint64) {
	waypointCheckpointLength.SetUint64(length)
}

func UpdateObservedWaypointMilestoneLength(length uint64) {
	waypointMilestoneLength.SetUint64(length)
}
