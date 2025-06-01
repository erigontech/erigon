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
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/poshttp"
)

const (
	StateEventsFetchLimit = 50
	SpansFetchLimit       = 150
	CheckpointsFetchLimit = 10_000
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
	fetchStateSyncEventsFormat = "from-id=%d&to-time=%d&limit=%d"
	fetchStateSyncEventsPath   = "clerk/event-record/list"
	fetchStateSyncEvent        = "clerk/event-record/%s"
)

func (c *HttpClient) FetchStateSyncEvents(ctx context.Context, fromID uint64, to time.Time, limit int) ([]*EventRecordWithTime, error) {
	eventRecords := make([]*EventRecordWithTime, 0)

	for {
		url, err := stateSyncListURL(c.UrlString, fromID, to.Unix())
		if err != nil {
			return nil, err
		}

		c.Logger.Trace(bridgeLogPrefix("Fetching state sync events"), "queryParams", url.RawQuery)

		reqCtx := poshttp.WithRequestType(ctx, poshttp.StateSyncRequest)

		response, err := poshttp.FetchWithRetry[StateSyncEventsResponse](reqCtx, c.Client, url, c.Logger)
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

func (c *HttpClient) FetchStateSyncEvent(ctx context.Context, id uint64) (*EventRecordWithTime, error) {
	url, err := stateSyncURL(c.UrlString, id)

	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.StateSyncRequest)

	isRecoverableError := func(err error) bool {
		return !strings.Contains(err.Error(), "could not get state record; No record found")
	}

	response, err := poshttp.FetchWithRetryEx[StateSyncEventResponse](ctx, c.Client, url, isRecoverableError, c.Logger)

	if err != nil {
		if strings.Contains(err.Error(), "could not get state record; No record found") {
			return nil, ErrEventRecordNotFound
		}

		return nil, err
	}

	return &response.Result, nil
}

func stateSyncListURL(urlString string, fromID uint64, to int64) (*url.URL, error) {
	queryParams := fmt.Sprintf(fetchStateSyncEventsFormat, fromID, to, StateEventsFetchLimit)
	return makeURL(urlString, fetchStateSyncEventsPath, queryParams)
}

func stateSyncURL(urlString string, id uint64) (*url.URL, error) {
	return makeURL(urlString, fmt.Sprintf(fetchStateSyncEvent, strconv.FormatUint(id, 10)), "")
}

func makeURL(urlString, rawPath, rawQuery string) (*url.URL, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, rawPath)
	u.RawQuery = rawQuery

	return u, err
}
