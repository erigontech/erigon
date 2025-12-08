package poshttp

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
	fetchChainManagerStatus = "/chainmanager/params"
)

const (
	apiHeimdallTimeout = 10 * time.Second
	retryBackOff       = time.Second
	maxRetries         = 5
)

type Client struct {
	UrlString    string
	handler      httpRequestHandler
	retryBackOff time.Duration
	maxRetries   int
	closeCh      chan struct{}
	Logger       log.Logger
	apiVersioner apiVersioner
	logPrefix    func(message string) string
}

type Request struct {
	handler httpRequestHandler
	url     *url.URL
	start   time.Time
}

type ClientOption func(*Client)

func WithHttpRequestHandler(handler httpRequestHandler) ClientOption {
	return func(client *Client) {
		client.handler = handler
	}
}

func WithHttpRetryBackOff(retryBackOff time.Duration) ClientOption {
	return func(client *Client) {
		client.retryBackOff = retryBackOff
	}
}

func WithHttpMaxRetries(maxRetries int) ClientOption {
	return func(client *Client) {
		client.maxRetries = maxRetries
	}
}

func WithApiVersioner(ctx context.Context) ClientOption {
	return func(client *Client) {
		client.apiVersioner = NewVersionMonitor(ctx, client, client.Logger, time.Minute)
	}
}

func NewClient(urlString string, logger log.Logger, logPrefix func(message string) string, opts ...ClientOption) *Client {
	c := &Client{
		UrlString:    urlString,
		Logger:       logger,
		handler:      &http.Client{Timeout: apiHeimdallTimeout},
		retryBackOff: retryBackOff,
		maxRetries:   maxRetries,
		closeCh:      make(chan struct{}),
		logPrefix:    logPrefix,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Client) Version() HeimdallVersion {
	if c.apiVersioner == nil {
		return HeimdallV1
	}
	return c.apiVersioner.Version()
}

// Close sends a signal to stop the running process
func (c *Client) Close() {
	close(c.closeCh)
	c.handler.CloseIdleConnections()
}

func (c *Client) FetchChainManagerStatus(ctx context.Context) (*ChainManagerStatus, error) {
	url, err := chainManagerStatusURL(c.UrlString)
	if err != nil {
		return nil, err
	}

	ctx = WithRequestType(ctx, StatusRequest)

	return FetchWithRetry[ChainManagerStatus](ctx, c, url, c.Logger)
}

func chainManagerStatusURL(urlString string) (*url.URL, error) {
	return MakeURL(urlString, fetchChainManagerStatus, "")
}

func MakeURL(urlString, rawPath, rawQuery string) (*url.URL, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, rawPath)
	u.RawQuery = rawQuery

	return u, err
}

// FetchWithRetry returns data from heimdall with retry
func FetchWithRetry[T any](ctx context.Context, client *Client, url *url.URL, logger log.Logger) (*T, error) {
	return FetchWithRetryEx[T](ctx, client, url, nil, logger)
}

// FetchWithRetryEx returns data from heimdall with retry
func FetchWithRetryEx[T any](
	ctx context.Context,
	client *Client,
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

		request := &Request{handler: client.handler, url: url, start: time.Now()}
		result, err = Fetch[T](ctx, request, logger, client.logPrefix, sendMetrics)
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
			client.Logger.Debug(client.logPrefix("service unavailable at the moment"), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt, "err", err)
			return nil, err
		}

		if (isRecoverableError != nil) && !isRecoverableError(err) {
			return nil, err
		}

		client.Logger.Debug(client.logPrefix("an error while fetching"), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt, "err", err)

		select {
		case <-ctx.Done():
			client.Logger.Debug(client.logPrefix("request canceled"), "reason", ctx.Err(), "path", url.Path, "queryParams", url.RawQuery, "attempt", attempt)
			return nil, ctx.Err()
		case <-client.closeCh:
			client.Logger.Debug(client.logPrefix("shutdown detected, terminating request"), "path", url.Path, "queryParams", url.RawQuery)
			return nil, ErrShutdownDetected
		case <-ticker.C:
			// retry
		}
	}

	return nil, err
}

// Fetch fetches response from heimdall
func Fetch[T any](ctx context.Context, request *Request, logger log.Logger, logPrefix func(string) string, sendMetrics func(ctx context.Context, start time.Time, isSuccessful bool)) (*T, error) {
	isSuccessful := false

	defer func() {
		if metrics.EnabledExpensive {
			sendMetrics(ctx, request.start, isSuccessful)
		}
	}()

	result := new(T)

	body, err := internalFetchWithTimeout(ctx, request.handler, request.url, logger, logPrefix)
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

// internal fetch method
func internalFetch(ctx context.Context, handler httpRequestHandler, u *url.URL, logger log.Logger, logPrefix func(string) string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	logger.Trace(logPrefix("http client get request"), "uri", u.RequestURI())

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

func internalFetchWithTimeout(ctx context.Context, handler httpRequestHandler, url *url.URL, logger log.Logger, logPrefix func(string) string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, apiHeimdallTimeout)
	defer cancel()

	// request data once
	return internalFetch(ctx, handler, url, logger, logPrefix)
}
