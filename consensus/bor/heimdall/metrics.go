package heimdall

import (
	"context"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

type (
	requestTypeKey struct{}
	requestType    string

	meter struct {
		request map[bool]*metrics.Counter // map[isSuccessful]metrics.Meter
		timer   metrics.Summary
	}
)

const (
	stateSyncRequest       requestType = "state-sync"
	spanRequest            requestType = "span"
	checkpointRequest      requestType = "checkpoint"
	checkpointCountRequest requestType = "checkpoint-count"
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
			request: map[bool]*metrics.Counter{
				true:  metrics.GetOrCreateCounter("client/requests/statesync/valid"),
				false: metrics.GetOrCreateCounter("client/requests/statesync/invalid"),
			},
			timer: *metrics.GetOrCreateSummary("client/requests/statesync/duration"),
		},
		spanRequest: {
			request: map[bool]*metrics.Counter{
				true:  metrics.GetOrCreateCounter("client/requests/span/valid"),
				false: metrics.GetOrCreateCounter("client/requests/span/invalid"),
			},
			timer: *metrics.GetOrCreateSummary("client/requests/span/duration"),
		},
		checkpointRequest: {
			request: map[bool]*metrics.Counter{
				true:  metrics.GetOrCreateCounter("client/requests/checkpoint/valid"),
				false: metrics.GetOrCreateCounter("client/requests/checkpoint/invalid"),
			},
			timer: *metrics.GetOrCreateSummary("client/requests/checkpoint/duration"),
		},
		checkpointCountRequest: {
			request: map[bool]*metrics.Counter{
				true:  metrics.GetOrCreateCounter("client/requests/checkpointcount/valid"),
				false: metrics.GetOrCreateCounter("client/requests/checkpointcount/invalid"),
			},
			timer: *metrics.GetOrCreateSummary("client/requests/checkpointcount/duration"),
		},
	}
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
	meters.timer.UpdateDuration(start)
}
