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
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/poshttp"
	"github.com/ethereum/go-ethereum/metrics"
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

	apiHeimdallTimeout = 30 * time.Second
	retryBackOff       = time.Second
	maxRetries         = 5
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

func WithHttpRetryBackOff(retryBackOff time.Duration) HttpClientOption {
	return func(client *HttpClient) {
		client.retryBackOff = retryBackOff
	}
}

func WithHttpMaxRetries(maxRetries int) HttpClientOption {
	return func(client *HttpClient) {
		client.maxRetries = maxRetries
	}
}

func WithApiVersioner(ctx context.Context) HttpClientOption {
	return func(client *HttpClient) {
		client.apiVersioner = NewVersionMonitor(ctx, client, client.logger, time.Minute)
	}
}

func NewHttpClient(urlString string, logger log.Logger, opts ...HttpClientOption) *HttpClient {
	c := &HttpClient{
		urlString:    urlString,
		logger:       logger,
		handler:      &http.Client{Timeout: apiHeimdallTimeout},
		retryBackOff: retryBackOff,
		maxRetries:   maxRetries,
		closeCh:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

const (
	fetchStateSyncEventsFormatV1 = "from-id=%d&to-time=%d&limit=%d"
	fetchStateSyncEventsFormatV2 = "from_id=%d&to_time=%s&pagination.limit=%d"
	fetchStateSyncEventsPathV1   = "clerk/event-record/list"
	fetchStateSyncEventsPathV2   = "clerk/time"

	fetchStatus             = "/status"
	fetchChainManagerStatus = "/chainmanager/params"

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
	ctx = withRequestType(ctx, spanRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		url, err := makeURL(c.urlString, fetchSpanLatestV2, "")
		if err != nil {
			return nil, err
		}

		response, err := FetchWithRetry[SpanResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return nil, err
		}

		return response.ToSpan()
	}

	url, err := makeURL(c.urlString, fetchSpanLatestV1, "")
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
	url, err := makeURL(c.urlString, fmt.Sprintf("bor/span/%d", spanID), "")
	if err != nil {
		return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.SpanRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		url, err = makeURL(c.urlString, fmt.Sprintf("bor/spans/%d", spanID), "")
		if err != nil {
			return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
		}

		response, err := FetchWithRetry[SpanResponseV2](ctx, c, url, c.logger)
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
	ctx = withRequestType(ctx, checkpointListRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		offset := (page - 1) * limit // page start from 1

		url, err := makeURL(c.urlString, fetchSpanListPathV2, fmt.Sprintf(fetchSpanListFormatV2, offset, limit))
		if err != nil {
			return nil, err
		}

		response, err := FetchWithRetry[SpanListResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return nil, err
		}

		return response.ToList()
	}

	url, err := makeURL(c.urlString, fetchSpanListPathV1, fmt.Sprintf(fetchSpanListFormatV1, page, limit))
	if err != nil {
		return nil, err
	}

	response, err := FetchWithRetry[SpanListResponseV1](ctx, c, url, c.logger)
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

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		response, err := FetchWithRetry[CheckpointResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return nil, err
		}

		return response.ToCheckpoint(number)
	}

	response, err := FetchWithRetry[CheckpointResponseV1](ctx, c, url, c.logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	ctx = withRequestType(ctx, checkpointListRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		offset := (page - 1) * limit // page start from 1

		url, err := makeURL(c.urlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormatV2, offset, limit))
		if err != nil {
			return nil, err
		}

		response, err := FetchWithRetry[CheckpointListResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return nil, err
		}

		return response.ToList()
	}

	url, err := makeURL(c.urlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormatV1, page, limit))
	if err != nil {
		return nil, err
	}

	response, err := FetchWithRetry[CheckpointListResponseV1](ctx, c, url, c.logger)
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
	url, err := milestoneURLv1(c.urlString, number)
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

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		url, err := milestoneURLv2(c.urlString, number)
		if err != nil {
			return nil, err
		}

		response, err := FetchWithRetryEx[MilestoneResponseV2](ctx, c, url, isRecoverableError, c.logger)
		if err != nil {
			if isInvalidMilestoneIndexError(err) {
				return nil, fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
			}
			return nil, err
		}

		return response.ToMilestone(number)
	}

	response, err := FetchWithRetryEx[MilestoneResponseV1](ctx, c, url, isRecoverableError, c.logger)
	if err != nil {
		if isInvalidMilestoneIndexError(err) {
			return nil, fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
		}
		return nil, err
	}

	response.Result.Id = MilestoneId(number)

	return &response.Result, nil
}

func (c *HttpClient) FetchChainManagerStatus(ctx context.Context) (*ChainManagerStatus, error) {
	url, err := chainManagerStatusURL(c.urlString)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, statusRequest)

	return FetchWithRetry[ChainManagerStatus](ctx, c, url, c.logger)
}

func (c *HttpClient) FetchStatus(ctx context.Context) (*Status, error) {
	url, err := statusURL(c.UrlString)
	if err != nil {
		return nil, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.StatusRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		return FetchWithRetry[Status](ctx, c, url, c.logger)
	}

	response, err := FetchWithRetry[StatusResponse](ctx, c, url, c.logger)
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

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		response, err := FetchWithRetry[CheckpointCountResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return 0, err
		}

		count, err := strconv.Atoi(response.AckCount)
		if err != nil {
			return 0, err
		}

		return int64(count), nil
	}

	response, err := FetchWithRetry[CheckpointCountResponseV1](ctx, c, url, c.logger)
	if err != nil {
		return 0, err
	}

	return response.Result.Result, nil
}

// FetchMilestoneCount fetches the milestone count from heimdall
func (c *HttpClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	url, err := makeURL(c.urlString, fetchMilestoneCountV1, "")
	if err != nil {
		return 0, err
	}

	ctx = poshttp.WithRequestType(ctx, poshttp.MilestoneCountRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		url, err := makeURL(c.urlString, fetchMilestoneCountV2, "")
		if err != nil {
			return 0, err
		}

		response, err := FetchWithRetry[MilestoneCountResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return 0, err
		}

		count, err := strconv.Atoi(response.Count)
		if err != nil {
			return 0, err
		}

		return int64(count), nil
	}

	response, err := FetchWithRetry[MilestoneCountResponseV1](ctx, c, url, c.logger)
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

// FetchWithRetry returns data from heimdall with retry
func FetchWithRetry[T any](ctx context.Context, client *HttpClient, url *url.URL, logger log.Logger) (*T, error) {
	return FetchWithRetryEx[T](ctx, client, url, nil, logger)
}

// FetchWithRetryEx returns data from heimdall with retry
func FetchWithRetryEx[T any](
	ctx context.Context,
	client *HttpClient,
	url *url.URL,
	isRecoverableError func(error) bool,
	logger log.Logger,
) (result *T, err error) {
	attempt := 0
	// create a new ticker for retrying the request
	ticker := time.NewTicker(client.retryBackOff)
	defer ticker.Stop()

	for attempt < client.maxRetries {
		attempt++

		request := &HttpRequest{handler: client.handler, url: url, start: time.Now()}
		result, err = Fetch[T](ctx, request, logger)
		if err == nil {
			return result, nil
		}

		if strings.Contains(err.Error(), "operation timed out") {
			return result, ErrOperationTimeout
		}

		if strings.Contains(err.Error(), "no such host") {
			return result, ErrNoHost
		}

		// 503 (Service Unavailable) is thrown when an endpoint isn't activated
		// yet in heimdall. E.g. when the hard fork hasn't hit yet but heimdall
		// is upgraded.
		if errors.Is(err, ErrServiceUnavailable) {
			client.logger.Debug(heimdallLogPrefix("service unavailable at the moment"), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt, "err", err)
			return nil, err
		}

		if (isRecoverableError != nil) && !isRecoverableError(err) {
			return nil, err
		}

		client.logger.Debug(heimdallLogPrefix("an error while fetching"), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt, "err", err)

		select {
		case <-ctx.Done():
			client.logger.Debug(heimdallLogPrefix("request canceled"), "reason", ctx.Err(), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt)
			return nil, ctx.Err()
		case <-client.closeCh:
			client.logger.Debug(heimdallLogPrefix("shutdown detected, terminating request"), "path", url.Path, "queryParams", url.RawQuery)
			return nil, ErrShutdownDetected
		case <-ticker.C:
			// retry
		}
	}

	return nil, err
}

// Fetch fetches response from heimdall
func Fetch[T any](ctx context.Context, request *HttpRequest, logger log.Logger) (*T, error) {
	isSuccessful := false

	defer func() {
		if metrics.EnabledExpensive {
			sendMetrics(ctx, request.start, isSuccessful)
		}
	}()

	result := new(T)

	body, err := internalFetchWithTimeout(ctx, request.handler, request.url, logger)
	if err != nil {
		return nil, err
	}

	if len(body) == 0 {
		return nil, ErrNoResponse
	}

	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, err
	}

	isSuccessful = true

	return result, nil
}

func stateSyncListURLv1(urlString string, fromID uint64, to int64) (*url.URL, error) {
	queryParams := fmt.Sprintf(fetchStateSyncEventsFormatV1, fromID, to, StateEventsFetchLimit)
	return makeURL(urlString, fetchStateSyncEventsPathV1, queryParams)
}

func stateSyncListURLv2(urlString string, fromID uint64, to int64) (*url.URL, error) {
	t := time.Unix(to, 0).UTC()
	formattedTime := t.Format(time.RFC3339Nano)

	queryParams := fmt.Sprintf(fetchStateSyncEventsFormatV2, fromID, formattedTime, StateEventsFetchLimit)
	return makeURL(urlString, fetchStateSyncEventsPathV2, queryParams)
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

func chainManagerStatusURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchChainManagerStatus, "")
}

func statusURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchStatus, "")
}

func milestoneURLv1(urlString string, number int64) (*url.URL, error) {
	if number == -1 {
		return makeURL(urlString, fetchMilestoneLatestV1, "")
	}
	return makeURL(urlString, fmt.Sprintf(fetchMilestoneAtV1, number), "")
}

func milestoneURLv2(urlString string, number int64) (*url.URL, error) {
	if number == -1 {
		return makeURL(urlString, fetchMilestoneLatestV2, "")
	}
	return makeURL(urlString, fmt.Sprintf(fetchMilestoneAtV2, number), "")
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
