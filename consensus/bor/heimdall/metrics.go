package heimdall

import (
	"context"
	"time"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/metrics/methelp"
	metrics "github.com/ledgerwatch/erigon/metrics/methelp"
)

type (
	requestTypeKey struct{}
	requestType    string

	meter struct {
		request map[bool]*metrics2.Counter // map[isSuccessful]metrics.Meter
		timer   *metrics2.Summary
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
			request: map[bool]*metrics2.Counter{
				true:  metrics.GetOrCreateCounter("client_requests_statesync_valid"),
				false: metrics.GetOrCreateCounter("client_requests_statesync_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_statesync_duration"),
		},
		spanRequest: {
			request: map[bool]*metrics2.Counter{
				true:  metrics.GetOrCreateCounter("client_requests_span_valid"),
				false: metrics.GetOrCreateCounter("client_requests_span_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_span_duration"),
		},
		checkpointRequest: {
			request: map[bool]*metrics2.Counter{
				true:  metrics.GetOrCreateCounter("client_requests_checkpoint_valid"),
				false: metrics.GetOrCreateCounter("client_requests_checkpoint_invalid"),
			},
			timer: methelp.GetOrCreateSummary("client_requests_checkpoint_duration"),
		},
		checkpointCountRequest: {
			request: map[bool]*metrics2.Counter{
				true:  metrics.GetOrCreateCounter("client_requests_checkpointcount_valid"),
				false: metrics.GetOrCreateCounter("client_requests_checkpointcount_invalid"),
			},
			timer: metrics.GetOrCreateSummary("client_requests_checkpointcount_duration"),
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
