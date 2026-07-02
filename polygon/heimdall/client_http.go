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
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
)

var (
	ErrNotInRejectedList  = errors.New("milestoneId doesn't exist in rejected list")
	ErrNotInMilestoneList = errors.New("milestoneId doesn't exist in Heimdall")
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
	fetchStatus                      = "/status"
	fetchCheckpoint                  = "/checkpoints/%s"
	fetchCheckpointCount             = "/checkpoints/count"
	fetchCheckpointList              = "/checkpoints/list"
	fetchCheckpointListQueryFormatV1 = "page=%d&limit=%d"
	fetchCheckpointListQueryFormatV2 = "pagination.offset=%d&pagination.limit=%d"

	fetchMilestoneAtV1     = "/milestone/%d"
	fetchMilestoneLatestV1 = "/milestone/latest"
	fetchMilestoneAtV2     = "/milestones/%d"
	fetchMilestoneLatestV2 = "/milestones/latest"
	fetchMilestoneCountV1  = "/milestone/count"
	fetchMilestoneCountV2  = "/milestones/count"

	fetchLastNoAckMilestone = "/milestone/lastNoAck"
	fetchNoAckMilestone     = "/milestone/noAck/%s"
	fetchMilestoneID        = "/milestone/ID/%s"

	fetchSpanLatestV1 = "bor/latest-span"
	fetchSpanLatestV2 = "bor/spans/latest"

	fetchSpanListFormatV1 = "page=%d&limit=%d" // max limit = 150
	fetchSpanListFormatV2 = "pagination.offset=%d&pagination.limit=%d"
	fetchSpanListPathV1   = "bor/span/list"
	fetchSpanListPathV2   = "bor/spans/list"
)

type fetchVersionedOpts struct {
	isRecoverableError func(error) bool
	wrapFetchErr       func(error) error
}

func (opts fetchVersionedOpts) wrapErr(err error) error {
	if opts.wrapFetchErr == nil {
		return err
	}
	return opts.wrapFetchErr(err)
}

func fetchVersioned[TV1, TV2, T any](
	ctx context.Context,
	c *HttpClient,
	urlV1, urlV2 *url.URL,
	fromV1 func(*TV1) (T, error),
	fromV2 func(*TV2) (T, error),
) (T, error) {
	return fetchVersionedEx(ctx, c, urlV1, urlV2, fromV1, fromV2, fetchVersionedOpts{})
}

func fetchVersionedEx[TV1, TV2, T any](
	ctx context.Context,
	c *HttpClient,
	urlV1, urlV2 *url.URL,
	fromV1 func(*TV1) (T, error),
	fromV2 func(*TV2) (T, error),
	opts fetchVersionedOpts,
) (T, error) {
	if c.Version() == poshttp.HeimdallV2 {
		response, err := poshttp.FetchWithRetryEx[TV2](ctx, c.Client, urlV2, opts.isRecoverableError, c.Logger)
		if err != nil {
			var zero T
			return zero, opts.wrapErr(err)
		}

		return fromV2(response)
	}

	response, err := poshttp.FetchWithRetryEx[TV1](ctx, c.Client, urlV1, opts.isRecoverableError, c.Logger)
	if err != nil {
		var zero T
		return zero, opts.wrapErr(err)
	}

	return fromV1(response)
}

func (c *HttpClient) FetchLatestSpan(ctx context.Context) (*Span, error) {
	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	urlV1, err := poshttp.MakeURL(c.UrlString, fetchSpanLatestV1, "")
	if err != nil {
		return nil, err
	}

	urlV2, err := poshttp.MakeURL(c.UrlString, fetchSpanLatestV2, "")
	if err != nil {
		return nil, err
	}

	return fetchVersioned(ctx, c, urlV1, urlV2,
		func(response *SpanResponseV1) (*Span, error) { return &response.Result, nil },
		(*SpanResponseV2).ToSpan,
	)
}

func (c *HttpClient) FetchSpan(ctx context.Context, spanID uint64) (*Span, error) {
	wrapErr := func(err error) error { return fmt.Errorf("%w, spanID=%d", err, spanID) }

	urlV1, err := poshttp.MakeURL(c.UrlString, fmt.Sprintf("bor/span/%d", spanID), "")
	if err != nil {
		return nil, wrapErr(err)
	}

	urlV2, err := poshttp.MakeURL(c.UrlString, fmt.Sprintf("bor/spans/%d", spanID), "")
	if err != nil {
		return nil, wrapErr(err)
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	return fetchVersionedEx(ctx, c, urlV1, urlV2,
		func(response *SpanResponseV1) (*Span, error) { return &response.Result, nil },
		(*SpanResponseV2).ToSpan,
		fetchVersionedOpts{wrapFetchErr: wrapErr},
	)
}

func (c *HttpClient) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error) {
	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointListRequest)

	urlV1, err := poshttp.MakeURL(c.UrlString, fetchSpanListPathV1, fmt.Sprintf(fetchSpanListFormatV1, page, limit))
	if err != nil {
		return nil, err
	}

	offset := (page - 1) * limit // page start from 1
	urlV2, err := poshttp.MakeURL(c.UrlString, fetchSpanListPathV2, fmt.Sprintf(fetchSpanListFormatV2, offset, limit))
	if err != nil {
		return nil, err
	}

	return fetchVersioned(ctx, c, urlV1, urlV2,
		func(response *SpanListResponseV1) ([]*Span, error) { return response.Result, nil },
		(*SpanListResponseV2).ToList,
	)
}

// FetchCheckpoint fetches the checkpoint from heimdall
func (c *HttpClient) FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error) {
	url, err := checkpointURL(c.UrlString, number)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointRequest)

	return fetchVersioned(ctx, c, url, url,
		func(response *CheckpointResponseV1) (*Checkpoint, error) { return &response.Result, nil },
		func(response *CheckpointResponseV2) (*Checkpoint, error) { return response.ToCheckpoint(number) },
	)
}

func (c *HttpClient) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointListRequest)

	urlV1, err := poshttp.MakeURL(c.UrlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormatV1, page, limit))
	if err != nil {
		return nil, err
	}

	offset := (page - 1) * limit // page start from 1
	urlV2, err := poshttp.MakeURL(c.UrlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormatV2, offset, limit))
	if err != nil {
		return nil, err
	}

	return fetchVersioned(ctx, c, urlV1, urlV2,
		func(response *CheckpointListResponseV1) ([]*Checkpoint, error) { return response.Result, nil },
		(*CheckpointListResponseV2).ToList,
	)
}

func isInvalidMilestoneIndexError(err error) bool {
	return errors.Is(err, poshttp.ErrNotSuccessfulResponse) &&
		strings.Contains(err.Error(), "Invalid milestone index")
}

// FetchMilestone fetches a milestone from heimdall
func (c *HttpClient) FetchMilestone(ctx context.Context, number int64) (*Milestone, error) {
	urlV1, err := milestoneURLv1(c.UrlString, number)
	if err != nil {
		return nil, err
	}

	urlV2, err := milestoneURLv2(c.UrlString, number)
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

	return fetchVersionedEx(ctx, c, urlV1, urlV2,
		func(response *MilestoneResponseV1) (*Milestone, error) {
			response.Result.Id = MilestoneId(number)
			return &response.Result, nil
		},
		func(response *MilestoneResponseV2) (*Milestone, error) { return response.ToMilestone(number) },
		fetchVersionedOpts{
			isRecoverableError: isRecoverableError,
			wrapFetchErr: func(err error) error {
				if isInvalidMilestoneIndexError(err) {
					return fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
				}
				return err
			},
		},
	)
}

func (c *HttpClient) FetchStatus(ctx context.Context) (*Status, error) {
	url, err := statusURL(c.UrlString)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.StatusRequest)

	return fetchVersioned(ctx, c, url, url,
		func(response *StatusResponse) (*Status, error) { return &response.Result, nil },
		func(status *Status) (*Status, error) { return status, nil },
	)
}

// FetchCheckpointCount fetches the checkpoint count from heimdall
func (c *HttpClient) FetchCheckpointCount(ctx context.Context) (int64, error) {
	url, err := checkpointCountURL(c.UrlString)
	if err != nil {
		return 0, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointCountRequest)

	return fetchVersioned(ctx, c, url, url,
		func(response *CheckpointCountResponseV1) (int64, error) { return response.Result.Result, nil },
		func(response *CheckpointCountResponseV2) (int64, error) { return parseCount(response.AckCount) },
	)
}

// FetchMilestoneCount fetches the milestone count from heimdall
func (c *HttpClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	urlV1, err := poshttp.MakeURL(c.UrlString, fetchMilestoneCountV1, "")
	if err != nil {
		return 0, err
	}

	urlV2, err := poshttp.MakeURL(c.UrlString, fetchMilestoneCountV2, "")
	if err != nil {
		return 0, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneCountRequest)

	return fetchVersioned(ctx, c, urlV1, urlV2,
		func(response *MilestoneCountResponseV1) (int64, error) { return response.Result.Count, nil },
		func(response *MilestoneCountResponseV2) (int64, error) { return parseCount(response.Count) },
	)
}

func parseCount(count string) (int64, error) {
	parsed, err := strconv.Atoi(count)
	if err != nil {
		return 0, err
	}

	return int64(parsed), nil
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

func checkpointURL(urlString string, number int64) (*url.URL, error) {
	var url string
	if number == -1 {
		url = fmt.Sprintf(fetchCheckpoint, "latest")
	} else {
		url = fmt.Sprintf(fetchCheckpoint, strconv.FormatInt(number, 10))
	}

	return poshttp.MakeURL(urlString, url, "")
}

func checkpointCountURL(urlString string) (*url.URL, error) {
	return poshttp.MakeURL(urlString, fetchCheckpointCount, "")
}

func statusURL(urlString string) (*url.URL, error) {
	return poshttp.MakeURL(urlString, fetchStatus, "")
}

func milestoneURLv1(urlString string, number int64) (*url.URL, error) {
	if number == -1 {
		return poshttp.MakeURL(urlString, fetchMilestoneLatestV1, "")
	}
	return poshttp.MakeURL(urlString, fmt.Sprintf(fetchMilestoneAtV1, number), "")
}

func milestoneURLv2(urlString string, number int64) (*url.URL, error) {
	if number == -1 {
		return poshttp.MakeURL(urlString, fetchMilestoneLatestV2, "")
	}
	return poshttp.MakeURL(urlString, fmt.Sprintf(fetchMilestoneAtV2, number), "")
}

func lastNoAckMilestoneURL(urlString string) (*url.URL, error) {
	return poshttp.MakeURL(urlString, fetchLastNoAckMilestone, "")
}

func noAckMilestoneURL(urlString string, id string) (*url.URL, error) {
	return poshttp.MakeURL(urlString, fmt.Sprintf(fetchNoAckMilestone, id), "")
}

func milestoneIDURL(urlString string, id string) (*url.URL, error) {
	return poshttp.MakeURL(urlString, fmt.Sprintf(fetchMilestoneID, id), "")
}
