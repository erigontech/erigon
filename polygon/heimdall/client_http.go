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
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/metrics"
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
	StateEventsFetchLimit = 50
	SpansFetchLimit       = 150
	CheckpointsFetchLimit = 10_000

	apiHeimdallTimeout = 10 * time.Second
	retryBackOff       = time.Second
	maxRetries         = 5
)

type apiVersioner interface {
	Version() HeimdallVersion
}

var _ Client = &HttpClient{}

type HttpClient struct {
	urlString    string
	handler      httpRequestHandler
	retryBackOff time.Duration
	maxRetries   int
	closeCh      chan struct{}
	logger       log.Logger
	apiVersioner apiVersioner
}

type HttpRequest struct {
	handler httpRequestHandler
	url     *url.URL
	start   time.Time
}

type HttpClientOption func(*HttpClient)

func WithHttpRequestHandler(handler httpRequestHandler) HttpClientOption {
	return func(client *HttpClient) {
		client.handler = handler
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
	fetchStateSyncEventsFormatV2 = "from_id=%d&to_time=%s&limit=%d"
	fetchStateSyncEventsPathV1   = "clerk/event-record/list"
	fetchStateSyncEventsPathV2   = "clerk/time"

	fetchStatus             = "/status"
	fetchChainManagerStatus = "/chainmanager/params"

	fetchCheckpoint                = "/checkpoints/%s"
	fetchCheckpointCount           = "/checkpoints/count"
	fetchCheckpointList            = "/checkpoints/list"
	fetchCheckpointListQueryFormat = "page=%d&limit=%d"

	fetchMilestoneAt      = "/milestone/%d"
	fetchMilestoneLatest  = "/milestone/latest"
	fetchMilestoneCountV1 = "/milestone/count"
	fetchMilestoneCountV2 = "/milestones/count"

	fetchLastNoAckMilestone = "/milestone/lastNoAck"
	fetchNoAckMilestone     = "/milestone/noAck/%s"
	fetchMilestoneID        = "/milestone/ID/%s"

	fetchSpanLatestV1 = "bor/latest-span"
	fetchSpanLatestV2 = "bor/spans/latest"

	fetchSpanListFormat = "page=%d&limit=%d" // max limit = 150
	fetchSpanListPath   = "bor/span/list"
)

func (c *HttpClient) FetchStateSyncEvents(ctx context.Context, fromID uint64, to time.Time, limit int) ([]*EventRecordWithTime, error) {
	eventRecords := make([]*EventRecordWithTime, 0)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		for {
			url, err := stateSyncListURLv2(c.urlString, fromID, to.Unix())
			if err != nil {
				return nil, err
			}

			c.logger.Trace(heimdallLogPrefix("Fetching state sync events"), "queryParams", url.RawQuery)

			reqCtx := withRequestType(ctx, stateSyncRequest)

			response, err := FetchWithRetry[StateSyncEventsResponseV2](reqCtx, c, url, c.logger)
			if err != nil {
				if errors.Is(err, ErrNoResponse) {
					// for more info check https://github.com/maticnetwork/heimdall/pull/993
					c.logger.Warn(
						heimdallLogPrefix("check heimdall logs to see if it is in sync - no response when querying state sync events"),
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

			eventRecords = append(eventRecords, response.EventRecords...)

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
		url, err := stateSyncListURLv1(c.urlString, fromID, to.Unix())
		if err != nil {
			return nil, err
		}

		c.logger.Trace(heimdallLogPrefix("Fetching state sync events"), "queryParams", url.RawQuery)

		reqCtx := withRequestType(ctx, stateSyncRequest)

		response, err := FetchWithRetry[StateSyncEventsResponseV1](reqCtx, c, url, c.logger)
		if err != nil {
			if errors.Is(err, ErrNoResponse) {
				// for more info check https://github.com/maticnetwork/heimdall/pull/993
				c.logger.Warn(
					heimdallLogPrefix("check heimdall logs to see if it is in sync - no response when querying state sync events"),
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

	response, err := FetchWithRetry[SpanResponseV1](ctx, c, url, c.logger)
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

	ctx = withRequestType(ctx, spanRequest)

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

	response, err := FetchWithRetry[SpanResponseV1](ctx, c, url, c.logger)
	if err != nil {
		return nil, fmt.Errorf("%w, spanID=%d", err, spanID)
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error) {
	url, err := spanListURL(c.urlString, page, limit)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, checkpointListRequest)

	response, err := FetchWithRetry[SpanListResponse](ctx, c, url, c.logger)
	if err != nil {
		return nil, err
	}

	return response.Result, nil
}

// FetchCheckpoint fetches the checkpoint from heimdall
func (c *HttpClient) FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error) {
	url, err := checkpointURL(c.urlString, number)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, checkpointRequest)

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		response, err := FetchWithRetry[CheckpointResponseV2](ctx, c, url, c.logger)
		if err != nil {
			return nil, err
		}

		return &response.Checkpoint, nil
	}

	response, err := FetchWithRetry[CheckpointResponseV1](ctx, c, url, c.logger)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func (c *HttpClient) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	url, err := checkpointListURL(c.urlString, page, limit)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, checkpointListRequest)

	response, err := FetchWithRetry[CheckpointListResponse](ctx, c, url, c.logger)
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
	url, err := milestoneURL(c.urlString, number)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, milestoneRequest)

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
			c.logger.Warn(
				heimdallLogPrefix("issue fetching milestone count when deciding if invalid index err is recoverable"),
				"err", err,
			)

			return false
		}

		// if number is within expected non pruned range then it should be retried
		return firstNum <= number && number <= firstNum+milestonePruneNumber-1
	}

	if c.apiVersioner != nil && c.apiVersioner.Version() == HeimdallV2 {
		response, err := FetchWithRetryEx[MilestoneResponseV2](ctx, c, url, isRecoverableError, c.logger)
		if err != nil {
			if isInvalidMilestoneIndexError(err) {
				return nil, fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
			}
			return nil, err
		}

		return &response.Milestone, nil
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
	url, err := statusURL(c.urlString)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, statusRequest)

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
	url, err := checkpointCountURL(c.urlString)
	if err != nil {
		return 0, err
	}

	ctx = withRequestType(ctx, checkpointCountRequest)

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

	ctx = withRequestType(ctx, milestoneCountRequest)

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
	url, err := lastNoAckMilestoneURL(c.urlString)
	if err != nil {
		return "", err
	}

	ctx = withRequestType(ctx, milestoneLastNoAckRequest)

	response, err := FetchWithRetry[MilestoneLastNoAckResponse](ctx, c, url, c.logger)
	if err != nil {
		return "", err
	}

	return response.Result.Result, nil
}

// FetchNoAckMilestone fetches the last no-ack-milestone from heimdall
func (c *HttpClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	url, err := noAckMilestoneURL(c.urlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = withRequestType(ctx, milestoneNoAckRequest)

	response, err := FetchWithRetry[MilestoneNoAckResponse](ctx, c, url, c.logger)
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
	url, err := milestoneIDURL(c.urlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = withRequestType(ctx, milestoneIDRequest)

	response, err := FetchWithRetry[MilestoneIDResponse](ctx, c, url, c.logger)

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

func spanListURL(urlString string, page, limit uint64) (*url.URL, error) {
	return makeURL(urlString, fetchSpanListPath, fmt.Sprintf(fetchSpanListFormat, page, limit))
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

func checkpointListURL(urlString string, page uint64, limit uint64) (*url.URL, error) {
	return makeURL(urlString, fetchCheckpointList, fmt.Sprintf(fetchCheckpointListQueryFormat, page, limit))
}

func milestoneURL(urlString string, number int64) (*url.URL, error) {
	if number == -1 {
		return makeURL(urlString, fetchMilestoneLatest, "")
	}
	return makeURL(urlString, fmt.Sprintf(fetchMilestoneAt, number), "")
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

// internal fetch method
func internalFetch(ctx context.Context, handler httpRequestHandler, u *url.URL, logger log.Logger) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	logger.Trace(heimdallLogPrefix("http client get request"), "uri", u.RequestURI())

	res, err := handler.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode == http.StatusServiceUnavailable {
		return nil, fmt.Errorf("%w: url='%s', status=%d", ErrServiceUnavailable, u.String(), res.StatusCode)
	}
	if res.StatusCode == http.StatusBadGateway {
		return nil, fmt.Errorf("%w: url='%s', status=%d", ErrBadGateway, u.String(), res.StatusCode)
	}

	// unmarshall data from buffer
	if res.StatusCode == 204 {
		return nil, nil
	}

	// get response
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	// check status code
	if res.StatusCode != 200 {
		cloudflareErr := regexp.MustCompile(`Error.*Cloudflare Access.*Unable to find your Access application`)
		bodyStr := string(body)
		if res.StatusCode == 404 && cloudflareErr.MatchString(bodyStr) {
			return nil, fmt.Errorf("%w: url='%s', status=%d, body='%s'", ErrCloudflareAccessNoApp, u.String(), res.StatusCode, bodyStr)
		}

		return nil, fmt.Errorf("%w: url='%s', status=%d, body='%s'", ErrNotSuccessfulResponse, u.String(), res.StatusCode, bodyStr)
	}

	return body, nil
}

func internalFetchWithTimeout(ctx context.Context, handler httpRequestHandler, url *url.URL, logger log.Logger) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, apiHeimdallTimeout)
	defer cancel()

	// request data once
	return internalFetch(ctx, handler, url, logger)
}

// Close sends a signal to stop the running process
func (c *HttpClient) Close() {
	close(c.closeCh)
	c.handler.CloseIdleConnections()
}
