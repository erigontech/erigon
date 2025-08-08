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

	"github.com/erigontech/erigon-lib/log/v3"
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

func (c *HttpClient) FetchLatestSpan(ctx context.Context) (*Span, error) {
	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	if c.Version() == poshttp.HeimdallV2 {
		url, err := poshttp.MakeURL(c.UrlString, fetchSpanLatestV2, "")
		if err != nil {
			return nil, err
		}
		response, err := poshttp.FetchWithRetry[SpanResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, err
		}

		return response.ToSpan()
	}

	url, err := poshttp.MakeURL(c.UrlString, fetchSpanLatestV1, "")
	if err != nil {
		return nil, err
	}

	response, err := poshttp.FetchWithRetry[SpanResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchSpan(ctx context.Context, spanID uint64) (*Span, error) {
	url, err := poshttp.MakeURL(c.UrlString, fmt.Sprintf("bor/span/%d", spanID), "")
	if err != nil {
		return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	if c.Version() == poshttp.HeimdallV2 {
		url, err = poshttp.MakeURL(c.UrlString, fmt.Sprintf("bor/spans/%d", spanID), "")
		if err != nil {
			return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
		}

		response, err := poshttp.FetchWithRetry[SpanResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
		}

		return response.ToSpan()

	}

	response, err := poshttp.FetchWithRetry[SpanResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error) {
	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointListRequest)

	if c.Version() == poshttp.HeimdallV2 {
		offset := (page - 1) * limit // page start from 1

		url, err := poshttp.MakeURL(c.UrlString, fetchSpanListPathV2, fmt.Sprintf(fetchSpanListFormatV2, offset, limit))
		if err != nil {
			return nil, err
		}

		response, err := poshttp.FetchWithRetry[SpanListResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, err
		}

		return response.ToList()
	}

	url, err := poshttp.MakeURL(c.UrlString, fetchSpanListPathV1, fmt.Sprintf(fetchSpanListFormatV1, page, limit))
	if err != nil {
		return nil, err
	}

	response, err := poshttp.FetchWithRetry[SpanListResponseV1](ctx, c.Client, url, c.Logger)
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

	if c.Version() == poshttp.HeimdallV2 {
		response, err := poshttp.FetchWithRetry[CheckpointResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, err
		}

		return response.ToCheckpoint(number)
	}

	response, err := poshttp.FetchWithRetry[CheckpointResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	ctx = poshttp.WithRequestType(ctx, poshttp.CheckpointListRequest)

	if c.Version() == poshttp.HeimdallV2 {
		offset := (page - 1) * limit // page start from 1

		url, err := poshttp.MakeURL(c.UrlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormatV2, offset, limit))
		if err != nil {
			return nil, err
		}

		response, err := poshttp.FetchWithRetry[CheckpointListResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return nil, err
		}

		return response.ToList()
	}

	url, err := poshttp.MakeURL(c.UrlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormatV1, page, limit))
	if err != nil {
		return nil, err
	}

	response, err := poshttp.FetchWithRetry[CheckpointListResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return nil, err
	}

	return response.Result, nil
}

func isInvalidMilestoneIndexError(err error) bool {
	return errors.Is(err, poshttp.ErrNotSuccessfulResponse) &&
		strings.Contains(err.Error(), "Invalid milestone index")
}

// FetchMilestone fetches a milestone from heimdall
func (c *HttpClient) FetchMilestone(ctx context.Context, number int64) (*Milestone, error) {
	url, err := milestoneURLv1(c.UrlString, number)
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

	if c.Version() == poshttp.HeimdallV2 {
		url, err := milestoneURLv2(c.UrlString, number)
		if err != nil {
			return nil, err
		}

		response, err := poshttp.FetchWithRetryEx[MilestoneResponseV2](ctx, c.Client, url, isRecoverableError, c.Logger)
		if err != nil {
			if isInvalidMilestoneIndexError(err) {
				return nil, fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
			}
			return nil, err
		}

		return response.ToMilestone(number)
	}

	response, err := poshttp.FetchWithRetryEx[MilestoneResponseV1](ctx, c.Client, url, isRecoverableError, c.Logger)
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

	if c.Version() == poshttp.HeimdallV2 {
		return poshttp.FetchWithRetry[Status](ctx, c.Client, url, c.Logger)
	}

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

	if c.Version() == poshttp.HeimdallV2 {
		response, err := poshttp.FetchWithRetry[CheckpointCountResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return 0, err
		}

		count, err := strconv.Atoi(response.AckCount)
		if err != nil {
			return 0, err
		}

		return int64(count), nil
	}

	response, err := poshttp.FetchWithRetry[CheckpointCountResponseV1](ctx, c.Client, url, c.Logger)
	if err != nil {
		return 0, err
	}

	return response.Result.Result, nil
}

// FetchMilestoneCount fetches the milestone count from heimdall
func (c *HttpClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	url, err := poshttp.MakeURL(c.UrlString, fetchMilestoneCountV1, "")
	if err != nil {
		return 0, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneCountRequest)

	if c.Version() == poshttp.HeimdallV2 {
		url, err := poshttp.MakeURL(c.UrlString, fetchMilestoneCountV2, "")
		if err != nil {
			return 0, err
		}

		response, err := poshttp.FetchWithRetry[MilestoneCountResponseV2](ctx, c.Client, url, c.Logger)
		if err != nil {
			return 0, err
		}

		count, err := strconv.Atoi(response.Count)
		if err != nil {
			return 0, err
		}

		return int64(count), nil
	}

	response, err := poshttp.FetchWithRetry[MilestoneCountResponseV1](ctx, c.Client, url, c.Logger)
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
