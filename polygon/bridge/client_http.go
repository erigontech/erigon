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

package bridge

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
)

const (
	StateEventsFetchLimit = 50
)

var _ Client = &HttpClient{}

type HttpClient struct {
	*poshttp.Client
}

func NewHttpClient(urlString string, logger log.Logger, opts ...poshttp.ClientOption) *HttpClient {
	return &HttpClient{
		poshttp.NewClient(urlString, logger, bridgeLogPrefix, opts...),
	}
}

const (
	fetchStateSyncEventsFormatV1 = "from-id=%d&to-time=%d&limit=%d"
	fetchStateSyncEventsFormatV2 = "from_id=%d&to_time=%s&pagination.limit=%d"
	fetchStateSyncEventsPathV1   = "clerk/event-record/list"
	fetchStateSyncEventsPathV2   = "clerk/time"
)

func (c *HttpClient) FetchStateSyncEvents(ctx context.Context, fromID uint64, to time.Time, limit int) ([]*EventRecordWithTime, error) {
	eventRecords := make([]*EventRecordWithTime, 0)

	if c.Version() == poshttp.HeimdallV2 {
		for {
			url, err := stateSyncListURLv2(c.UrlString, fromID, to.Unix())
			if err != nil {
				return nil, err
			}

			c.Logger.Trace(bridgeLogPrefix("Fetching state sync events"), "queryParams", url.RawQuery)

			reqCtx := poshttp.WithRequestType(ctx, poshttp.StateSyncRequest)

			response, err := poshttp.FetchWithRetry[StateSyncEventsResponseV2](reqCtx, c.Client, url, c.Logger)
			if err != nil {
				if errors.Is(err, poshttp.ErrNoResponse) {
					// for more info check https://github.com/maticnetwork/heimdall/pull/993
					c.Logger.Warn(
						bridgeLogPrefix("check heimdall logs to see if it is in sync - no response when querying state sync events"),
						"path", url.Path,
						"queryParams", url.RawQuery,
					)
				}
				return nil, err
			}

			if response == nil || response.EventRecords == nil {
				// status 204
				break
			}

			records, err := response.GetEventRecords()
			if err != nil {
				return nil, err
			}

			eventRecords = append(eventRecords, records...)

			if len(response.EventRecords) < StateEventsFetchLimit || (limit > 0 && len(eventRecords) >= limit) {
				break
			}

			fromID += uint64(StateEventsFetchLimit)
		}

		sort.SliceStable(eventRecords, func(i, j int) bool {
			return eventRecords[i].ID < eventRecords[j].ID
		})

		return eventRecords, nil
	}

	for {
		url, err := stateSyncListURLv1(c.UrlString, fromID, to.Unix())
		if err != nil {
			return nil, err
		}

		c.Logger.Trace(bridgeLogPrefix("Fetching state sync events"), "queryParams", url.RawQuery)

		reqCtx := poshttp.WithRequestType(ctx, poshttp.StateSyncRequest)

		response, err := poshttp.FetchWithRetry[StateSyncEventsResponseV1](reqCtx, c.Client, url, c.Logger)
		if err != nil {
			if errors.Is(err, poshttp.ErrNoResponse) {
				// for more info check https://github.com/maticnetwork/heimdall/pull/993
				c.Logger.Warn(
					bridgeLogPrefix("check heimdall logs to see if it is in sync - no response when querying state sync events"),
					"path", url.Path,
					"queryParams", url.RawQuery,
				)
			}
			return nil, err
		}

		if response == nil || response.Result == nil {
			// status 204
			break
		}

		eventRecords = append(eventRecords, response.Result...)

		if len(response.Result) < StateEventsFetchLimit || (limit > 0 && len(eventRecords) >= limit) {
			break
		}

		fromID += uint64(StateEventsFetchLimit)
	}

	sort.SliceStable(eventRecords, func(i, j int) bool {
		return eventRecords[i].ID < eventRecords[j].ID
	})

	return eventRecords, nil
}

func stateSyncListURLv1(urlString string, fromID uint64, to int64) (*url.URL, error) {
	queryParams := fmt.Sprintf(fetchStateSyncEventsFormatV1, fromID, to, StateEventsFetchLimit)
	return poshttp.MakeURL(urlString, fetchStateSyncEventsPathV1, queryParams)
}

func stateSyncListURLv2(urlString string, fromID uint64, to int64) (*url.URL, error) {
	t := time.Unix(to, 0).UTC()
	formattedTime := t.Format(time.RFC3339Nano)

	queryParams := fmt.Sprintf(fetchStateSyncEventsFormatV2, fromID, formattedTime, StateEventsFetchLimit)
	return poshttp.MakeURL(urlString, fetchStateSyncEventsPathV2, queryParams)
}
