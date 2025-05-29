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
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/poshttp"
)

var (
	// ErrShutdownDetected is returned if a shutdown was detected
	ErrShutdownDetected      = errors.New("shutdown detected")
	ErrNoResponse            = errors.New("got a nil response")
	ErrNotSuccessfulResponse = errors.New("error while fetching data from Heimdall")
	ErrNotInRejectedList     = errors.New("milestoneId doesn't exist in rejected list")
	ErrNotInMilestoneList    = errors.New("milestoneId doesn't exist in Heimdall")
	ErrNotInCheckpointList   = errors.New("checkpontId doesn't exist in Heimdall")
	ErrBadGateway            = errors.New("bad gateway")
	ErrServiceUnavailable    = errors.New("service unavailable")
	ErrCloudflareAccessNoApp = errors.New("cloudflare access - no application")
	ErrOperationTimeout      = errors.New("operation timed out, check internet connection")
	ErrNoHost                = errors.New("no such host, check internet connection")

	TransientErrors = []error{
		ErrBadGateway,
		ErrServiceUnavailable,
		ErrCloudflareAccessNoApp,
		ErrOperationTimeout,
		ErrNoHost,
		context.DeadlineExceeded,
	}
)

const (
	SpansFetchLimit       = 150
	CheckpointsFetchLimit = 10_000
)

var _ Client = &HttpClient{}

type HttpClient struct {
	*poshttp.Client
}

func NewHttpClient(urlString string, logger log.Logger, opts ...poshttp.ClientOption) *HttpClient {
	return &HttpClient{
		poshttp.NewClient(urlString, logger, heimdallLogPrefix, opts...),
	}
}

const (
	fetchStateSyncEvent = "clerk/event-record/%s"

	fetchStatus = "/status"

	fetchCheckpoint                = "/checkpoints/%s"
	fetchCheckpointCount           = "/checkpoints/count"
	fetchCheckpointList            = "/checkpoints/list"
	fetchCheckpointListQueryFormat = "page=%d&limit=%d"

	fetchMilestoneAt     = "/milestone/%d"
	fetchMilestoneLatest = "/milestone/latest"
	fetchMilestoneCount  = "/milestone/count"

	fetchLastNoAckMilestone = "/milestone/lastNoAck"
	fetchNoAckMilestone     = "/milestone/noAck/%s"
	fetchMilestoneID        = "/milestone/ID/%s"

	fetchSpanFormat     = "bor/span/%d"
	fetchSpanLatest     = "bor/latest-span"
	fetchSpanListFormat = "page=%d&limit=%d" // max limit = 150
	fetchSpanListPath   = "bor/span/list"
)

func (c *HttpClient) FetchLatestSpan(ctx context.Context) (*Span, error) {
	url, err := latestSpanURL(c.UrlString)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	if c.Version() == poshttp.HeimdallV2 {
		response, err := poshttp.FetchWithRetry[SpanResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, err
		}

		return response.Span, nil
	}

	response, err := poshttp.FetchWithRetry[SpanResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchSpan(ctx context.Context, spanID uint64) (*Span, error) {
	url, err := spanURL(c.UrlString, spanID)
	if err != nil {
		return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	if c.Version() == poshttp.HeimdallV2 {
		response, err := poshttp.FetchWithRetry[SpanResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
		}

		return response.Span, nil

	}

	response, err := poshttp.FetchWithRetry[SpanResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error) {
	url, err := spanListURL(c.UrlString, page, limit)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointListRequest)

	response, err := poshttp.FetchWithRetry[SpanListResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return response.Result, nil
}

// FetchCheckpoint fetches the checkpoint from heimdall
func (c *HttpClient) FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error) {
	url, err := checkpointURL(c.UrlString, number)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointRequest)

	response, err := poshttp.FetchWithRetry[CheckpointResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	url, err := checkpointListURL(c.UrlString, page, limit)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointListRequest)

	response, err := poshttp.FetchWithRetry[CheckpointListResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return response.Result, nil
}

func isInvalidMilestoneIndexError(err error) bool {
	return errors.Is(err, ErrNotSuccessfulResponse) &&
		strings.Contains(err.Error(), "Invalid milestone index")
}

// FetchMilestone fetches a milestone from heimdall
func (c *HttpClient) FetchMilestone(ctx context.Context, number int64) (*Milestone, error) {
	url, err := milestoneURL(c.UrlString, number)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneRequest)

	isRecoverableError := func(err error) bool {
		if !isInvalidMilestoneIndexError(err) {
			return true
		}

		if number == -1 {
			// -1 means fetch latest, which should be retried
			return true
		}

		firstNum, err := c.FetchFirstMilestoneNum(ctx)
		if err != nil {
			c.Logger.Warn(
				heimdallLogPrefix("issue fetching milestone count when deciding if invalid index err is recoverable"),
				"err", err,
			)

			return false
		}

		// if number is within expected non pruned range then it should be retried
		return firstNum <= number && number <= firstNum+milestonePruneNumber-1
	}

	response, err := poshttp.FetchWithRetryEx[MilestoneResponse](ctx, c.Client, url, isRecoverableError, c.Logger)
	if err != nil {
		if isInvalidMilestoneIndexError(err) {
			return nil, fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
		}
		return nil, err
	}

	response.Result.Id = MilestoneId(number)

	return &response.Result, nil
}

func (c *HttpClient) FetchStatus(ctx context.Context) (*Status, error) {
	url, err := statusURL(c.UrlString)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.StatusRequest)

	response, err := poshttp.FetchWithRetry[StatusResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

// FetchCheckpointCount fetches the checkpoint count from heimdall
func (c *HttpClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	url, err := checkpointCountURL(c.UrlString)
	if err != nil {
		return 0, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointCountRequest)

	response, err := poshttp.FetchWithRetry[CheckpointCountResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return 0, err
	}

	return response.Result.Result, nil
}

// FetchMilestoneCount fetches the milestone count from heimdall
func (c *HttpClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	url, err := milestoneCountURL(c.UrlString)
	if err != nil {
		return 0, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneCountRequest)

	response, err := poshttp.FetchWithRetry[MilestoneCountResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return 0, err
	}

	return response.Result.Count, nil
}

// Heimdall keeps only this amount of latest milestones
// https://github.com/maticnetwork/heimdall/blob/master/helper/config.go#L141
const milestonePruneNumber = int64(100)

func (c *HttpClient) FetchFirstMilestoneNum(ctx context.Context) (int64, error) {
	count, err := c.FetchMilestoneCount(ctx)
	if err != nil {
		return 0, err
	}

	var first int64
	if count < milestonePruneNumber {
		first = 1
	} else {
		first = count - milestonePruneNumber + 1
	}

	return first, nil
}

// FetchLastNoAckMilestone fetches the last no-ack-milestone from heimdall
func (c *HttpClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	url, err := lastNoAckMilestoneURL(c.UrlString)
	if err != nil {
		return "", err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneLastNoAckRequest)

	response, err := poshttp.FetchWithRetry[MilestoneLastNoAckResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return "", err
	}

	return response.Result.Result, nil
}

// FetchNoAckMilestone fetches the last no-ack-milestone from heimdall
func (c *HttpClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	url, err := noAckMilestoneURL(c.UrlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneNoAckRequest)

	response, err := poshttp.FetchWithRetry[MilestoneNoAckResponse](ctx, c.Client, url, c.Logger)
	if err != nil {
		return err
	}

	if !response.Result.Result {
		return fmt.Errorf("%w: milestoneID %q", ErrNotInRejectedList, milestoneID)
	}

	return nil
}

// FetchMilestoneID fetches the bool result from Heimdall whether the ID corresponding
// to the given milestone is in process in Heimdall
func (c *HttpClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	url, err := milestoneIDURL(c.UrlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneIDRequest)

	response, err := poshttp.FetchWithRetry[MilestoneIDResponse](ctx, c.Client, url, c.Logger)

	if err != nil {
		return err
	}

	if !response.Result.Result {
		return fmt.Errorf("%w: milestoneID %q", ErrNotInMilestoneList, milestoneID)
	}

	return nil
}

func spanURL(urlString string, spanID uint64) (*url.URL, error) {
	return makeURL(urlString, fmt.Sprintf(fetchSpanFormat, spanID), "")
}

func spanListURL(urlString string, page, limit uint64) (*url.URL, error) {
	return makeURL(urlString, fetchSpanListPath, fmt.Sprintf(fetchSpanListFormat, page, limit))
}

func latestSpanURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchSpanLatest, "")
}

func checkpointURL(urlString string, number int64) (*url.URL, error) {
	var url string
	if number == -1 {
		url = fmt.Sprintf(fetchCheckpoint, "latest")
	} else {
		url = fmt.Sprintf(fetchCheckpoint, strconv.FormatInt(number, 10))
	}

	return makeURL(urlString, url, "")
}

func checkpointCountURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchCheckpointCount, "")
}

func statusURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchStatus, "")
}

func checkpointListURL(urlString string, page uint64, limit uint64) (*url.URL, error) {
	return makeURL(urlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormat, page, limit))
}

func milestoneURL(urlString string, number int64) (*url.URL, error) {
	if number == -1 {
		return makeURL(urlString, fetchMilestoneLatest, "")
	}
	return makeURL(urlString, fmt.Sprintf(fetchMilestoneAt, number), "")
}

func milestoneCountURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchMilestoneCount, "")
}

func lastNoAckMilestoneURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchLastNoAckMilestone, "")
}

func noAckMilestoneURL(urlString string, id string) (*url.URL, error) {
	return makeURL(urlString, fmt.Sprintf(fetchNoAckMilestone, id), "")
}

func milestoneIDURL(urlString string, id string) (*url.URL, error) {
	return makeURL(urlString, fmt.Sprintf(fetchMilestoneID, id), "")
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
