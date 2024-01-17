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
	"sort"
	"strings"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

var (
	// ErrShutdownDetected is returned if a shutdown was detected
	ErrShutdownDetected      = errors.New("shutdown detected")
	ErrNoResponse            = errors.New("got a nil response")
	ErrNotSuccessfulResponse = errors.New("error while fetching data from Heimdall")
	ErrNotInRejectedList     = errors.New("milestoneID doesn't exist in rejected list")
	ErrNotInMilestoneList    = errors.New("milestoneID doesn't exist in Heimdall")
	ErrServiceUnavailable    = errors.New("service unavailable")
)

const (
	stateFetchLimit    = 50
	apiHeimdallTimeout = 10 * time.Second
	retryBackOff       = time.Second
	maxRetries         = 5
)

//go:generate mockgen -destination=./client_mock.go -package=heimdall . HeimdallClient
type HeimdallClient interface {
	StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*EventRecordWithTime, error)
	Span(ctx context.Context, spanID uint64) (*HeimdallSpan, error)
	FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	FetchMilestone(ctx context.Context, number int64) (*Milestone, error)
	FetchMilestoneCount(ctx context.Context) (int64, error)

	// FetchNoAckMilestone fetches a bool value whether milestone corresponding to the given id failed in the Heimdall
	FetchNoAckMilestone(ctx context.Context, milestoneID string) error

	// FetchLastNoAckMilestone fetches the latest failed milestone id
	FetchLastNoAckMilestone(ctx context.Context) (string, error)

	// FetchMilestoneID fetches a bool value whether milestone corresponding to the given id is in process in Heimdall
	FetchMilestoneID(ctx context.Context, milestoneID string) error

	Close()
}

type Client struct {
	urlString    string
	client       HttpClient
	retryBackOff time.Duration
	maxRetries   int
	closeCh      chan struct{}
	logger       log.Logger
}

type Request struct {
	client HttpClient
	url    *url.URL
	start  time.Time
}

//go:generate mockgen -destination=./http_client_mock.go -package=heimdall . HttpClient
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
	CloseIdleConnections()
}

func NewHeimdallClient(urlString string, logger log.Logger) *Client {
	httpClient := &http.Client{
		Timeout: apiHeimdallTimeout,
	}
	return newHeimdallClient(urlString, httpClient, retryBackOff, maxRetries, logger)
}

func newHeimdallClient(urlString string, httpClient HttpClient, retryBackOff time.Duration, maxRetries int, logger log.Logger) *Client {
	return &Client{
		urlString:    urlString,
		logger:       logger,
		client:       httpClient,
		retryBackOff: retryBackOff,
		maxRetries:   maxRetries,
		closeCh:      make(chan struct{}),
	}
}

const (
	fetchStateSyncEventsFormat = "from-id=%d&to-time=%d&limit=%d"
	fetchStateSyncEventsPath   = "clerk/event-record/list"

	fetchCheckpoint      = "/checkpoints/%s"
	fetchCheckpointCount = "/checkpoints/count"

	fetchMilestoneAt     = "/milestone/%d"
	fetchMilestoneLatest = "/milestone/latest"
	fetchMilestoneCount  = "/milestone/count"

	fetchLastNoAckMilestone = "/milestone/lastNoAck"
	fetchNoAckMilestone     = "/milestone/noAck/%s"
	fetchMilestoneID        = "/milestone/ID/%s"

	fetchSpanFormat = "bor/span/%d"
)

func (c *Client) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*EventRecordWithTime, error) {
	eventRecords := make([]*EventRecordWithTime, 0)

	for {
		url, err := stateSyncURL(c.urlString, fromID, to)
		if err != nil {
			return nil, err
		}

		c.logger.Debug("[bor.heimdall] Fetching state sync events", "queryParams", url.RawQuery)

		ctx = withRequestType(ctx, stateSyncRequest)

		response, err := FetchWithRetry[StateSyncEventsResponse](ctx, c, url)
		if err != nil {
			if errors.Is(err, ErrNoResponse) {
				// for more info check https://github.com/maticnetwork/heimdall/pull/993
				c.logger.Warn(
					"[bor.heimdall] check heimdall logs to see if it is in sync - no response when querying state sync events",
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

		if len(response.Result) < stateFetchLimit {
			break
		}

		fromID += uint64(stateFetchLimit)
	}

	sort.SliceStable(eventRecords, func(i, j int) bool {
		return eventRecords[i].ID < eventRecords[j].ID
	})

	return eventRecords, nil
}

func (c *Client) Span(ctx context.Context, spanID uint64) (*HeimdallSpan, error) {
	url, err := spanURL(c.urlString, spanID)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, spanRequest)

	response, err := FetchWithRetry[SpanResponse](ctx, c, url)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

// FetchCheckpoint fetches the checkpoint from heimdall
func (c *Client) FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error) {
	url, err := checkpointURL(c.urlString, number)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, checkpointRequest)

	response, err := FetchWithRetry[CheckpointResponse](ctx, c, url)
	if err != nil {
		return nil, err
	}

	return &response.Result, nil
}

func isInvalidMilestoneIndexError(err error) bool {
	return errors.Is(err, ErrNotSuccessfulResponse) &&
		strings.Contains(err.Error(), "Invalid milestone index")
}

// FetchMilestone fetches a milestone from heimdall
func (c *Client) FetchMilestone(ctx context.Context, number int64) (*Milestone, error) {
	url, err := milestoneURL(c.urlString, number)
	if err != nil {
		return nil, err
	}

	ctx = withRequestType(ctx, milestoneRequest)

	isRecoverableError := func(err error) bool {
		return !isInvalidMilestoneIndexError(err)
	}

	response, err := FetchWithRetryEx[MilestoneResponse](ctx, c, url, isRecoverableError)
	if err != nil {
		if isInvalidMilestoneIndexError(err) {
			return nil, fmt.Errorf("%w: number %d", ErrNotInMilestoneList, number)
		}
		return nil, err
	}

	return &response.Result, nil
}

// FetchCheckpointCount fetches the checkpoint count from heimdall
func (c *Client) FetchCheckpointCount(ctx context.Context) (int64, error) {
	url, err := checkpointCountURL(c.urlString)
	if err != nil {
		return 0, err
	}

	ctx = withRequestType(ctx, checkpointCountRequest)

	response, err := FetchWithRetry[CheckpointCountResponse](ctx, c, url)
	if err != nil {
		return 0, err
	}

	return response.Result.Result, nil
}

// FetchMilestoneCount fetches the milestone count from heimdall
func (c *Client) FetchMilestoneCount(ctx context.Context) (int64, error) {
	url, err := milestoneCountURL(c.urlString)
	if err != nil {
		return 0, err
	}

	ctx = withRequestType(ctx, milestoneCountRequest)

	response, err := FetchWithRetry[MilestoneCountResponse](ctx, c, url)
	if err != nil {
		return 0, err
	}

	return response.Result.Count, nil
}

// FetchLastNoAckMilestone fetches the last no-ack-milestone from heimdall
func (c *Client) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	url, err := lastNoAckMilestoneURL(c.urlString)
	if err != nil {
		return "", err
	}

	ctx = withRequestType(ctx, milestoneLastNoAckRequest)

	response, err := FetchWithRetry[MilestoneLastNoAckResponse](ctx, c, url)
	if err != nil {
		return "", err
	}

	return response.Result.Result, nil
}

// FetchNoAckMilestone fetches the last no-ack-milestone from heimdall
func (c *Client) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	url, err := noAckMilestoneURL(c.urlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = withRequestType(ctx, milestoneNoAckRequest)

	response, err := FetchWithRetry[MilestoneNoAckResponse](ctx, c, url)
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
func (c *Client) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	url, err := milestoneIDURL(c.urlString, milestoneID)
	if err != nil {
		return err
	}

	ctx = withRequestType(ctx, milestoneIDRequest)

	response, err := FetchWithRetry[MilestoneIDResponse](ctx, c, url)

	if err != nil {
		return err
	}

	if !response.Result.Result {
		return fmt.Errorf("%w: milestoneID %q", ErrNotInMilestoneList, milestoneID)
	}

	return nil
}

// FetchWithRetry returns data from heimdall with retry
func FetchWithRetry[T any](ctx context.Context, client *Client, url *url.URL) (*T, error) {
	return FetchWithRetryEx[T](ctx, client, url, nil)
}

// FetchWithRetryEx returns data from heimdall with retry
func FetchWithRetryEx[T any](ctx context.Context, client *Client, url *url.URL, isRecoverableError func(error) bool) (result *T, err error) {
	attempt := 0
	// create a new ticker for retrying the request
	ticker := time.NewTicker(client.retryBackOff)
	defer ticker.Stop()

	for attempt < client.maxRetries {
		attempt++

		request := &Request{client: client.client, url: url, start: time.Now()}
		result, err = Fetch[T](ctx, request)
		if err == nil {
			return result, nil
		}

		// 503 (Service Unavailable) is thrown when an endpoint isn't activated
		// yet in heimdall. E.g. when the hard fork hasn't hit yet but heimdall
		// is upgraded.
		if errors.Is(err, ErrServiceUnavailable) {
			client.logger.Debug("[bor.heimdall] service unavailable at the moment", "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt, "err", err)
			return nil, err
		}

		if (isRecoverableError != nil) && !isRecoverableError(err) {
			return nil, err
		}

		client.logger.Warn("[bor.heimdall] an error while fetching", "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt, "err", err)

		select {
		case <-ctx.Done():
			client.logger.Debug("[bor.heimdall] request canceled", "reason", ctx.Err(), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt)
			return nil, ctx.Err()
		case <-client.closeCh:
			client.logger.Debug("[bor.heimdall] shutdown detected, terminating request", "path", url.Path, "queryParams", url.RawQuery)
			return nil, ErrShutdownDetected
		case <-ticker.C:
			// retry
		}
	}

	return nil, err
}

// Fetch fetches response from heimdall
func Fetch[T any](ctx context.Context, request *Request) (*T, error) {
	isSuccessful := false

	defer func() {
		if metrics.EnabledExpensive {
			sendMetrics(ctx, request.start, isSuccessful)
		}
	}()

	result := new(T)

	body, err := internalFetchWithTimeout(ctx, request.client, request.url)
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

func spanURL(urlString string, spanID uint64) (*url.URL, error) {
	return makeURL(urlString, fmt.Sprintf(fetchSpanFormat, spanID), "")
}

func stateSyncURL(urlString string, fromID uint64, to int64) (*url.URL, error) {
	queryParams := fmt.Sprintf(fetchStateSyncEventsFormat, fromID, to, stateFetchLimit)

	return makeURL(urlString, fetchStateSyncEventsPath, queryParams)
}

func checkpointURL(urlString string, number int64) (*url.URL, error) {
	url := ""
	if number == -1 {
		url = fmt.Sprintf(fetchCheckpoint, "latest")
	} else {
		url = fmt.Sprintf(fetchCheckpoint, fmt.Sprint(number))
	}

	return makeURL(urlString, url, "")
}

func checkpointCountURL(urlString string) (*url.URL, error) {
	return makeURL(urlString, fetchCheckpointCount, "")
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

// internal fetch method
func internalFetch(ctx context.Context, client HttpClient, u *url.URL) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode == http.StatusServiceUnavailable {
		return nil, fmt.Errorf("%w: url='%s', status=%d", ErrServiceUnavailable, u.String(), res.StatusCode)
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
		return nil, fmt.Errorf("%w: url='%s', status=%d, body='%s'", ErrNotSuccessfulResponse, u.String(), res.StatusCode, string(body))
	}

	return body, nil
}

func internalFetchWithTimeout(ctx context.Context, client HttpClient, url *url.URL) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, apiHeimdallTimeout)
	defer cancel()

	// request data once
	return internalFetch(ctx, client, url)
}

// Close sends a signal to stop the running process
func (c *Client) Close() {
	close(c.closeCh)
	c.client.CloseIdleConnections()
}
