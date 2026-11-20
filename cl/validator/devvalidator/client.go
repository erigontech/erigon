package devvalidator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"time"
)

// BeaconClient is a minimal HTTP client for the Beacon API.
// It talks to the same endpoints that Lighthouse/Teku use.
type BeaconClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewBeaconClient creates a client pointing at the given Beacon API URL.
func NewBeaconClient(baseURL string) *BeaconClient {
	return &BeaconClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// beaconResponse is the standard Beacon API wrapper.
type beaconResponse struct {
	Data json.RawMessage `json:"data"`
}

// get performs a GET request and unmarshals the `data` field into dst.
func (c *BeaconClient) get(ctx context.Context, path string, dst any) error {
	wrapper, err := c.getResponse(ctx, path)
	if err != nil {
		return err
	}
	return json.Unmarshal(wrapper.Data, dst)
}

func (c *BeaconClient) getResponse(ctx context.Context, path string) (*beaconResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return c.doResponse(req, path)
}

// postResponse performs a POST request with a JSON body and returns the decoded response.
func (c *BeaconClient) postResponse(ctx context.Context, path, body string, headers http.Header) (*beaconResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	maps.Copy(req.Header, headers)
	return c.doResponse(req, path)
}

func (c *BeaconClient) doResponse(req *http.Request, path string) (*beaconResponse, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("beacon %s %s: %w", req.Method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, &statusError{method: req.Method, path: path, code: resp.StatusCode, body: string(body)}
	}

	var wrapper beaconResponse
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		return nil, fmt.Errorf("beacon %s %s: decode: %w", req.Method, path, err)
	}
	return &wrapper, nil
}

// statusError is a non-200 response to a Beacon API request.
type statusError struct {
	method string
	path   string
	code   int
	body   string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("beacon %s %s: status %d: %s", e.method, e.path, e.code, e.body)
}

// post performs a POST request with a JSON body.
func (c *BeaconClient) post(ctx context.Context, path string, body any) error {
	url := c.baseURL + path
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(jsonBody)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("beacon POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("beacon POST %s: status %d: %s", path, resp.StatusCode, string(body))
	}
	return nil
}

// postAndDecode performs a POST request with a JSON body and unmarshals the
// response `data` field into dst. This is needed for endpoints like
// /eth/v1/validator/duties/attester/{epoch} which are POST-only per spec.
func (c *BeaconClient) postAndDecode(ctx context.Context, path string, body any, dst any) error {
	url := c.baseURL + path
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(jsonBody)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("beacon POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("beacon POST %s: status %d: %s", path, resp.StatusCode, string(respBody))
	}

	var wrapper beaconResponse
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		return fmt.Errorf("beacon POST %s: decode: %w", path, err)
	}
	return json.Unmarshal(wrapper.Data, dst)
}

// postJSON performs a POST request with a JSON body and Eth-Consensus-Version header.
func (c *BeaconClient) postJSON(ctx context.Context, path string, body any, version string) error {
	headers := make(http.Header)
	if version != "" {
		headers.Set("Eth-Consensus-Version", version)
	}
	return c.postJSONWithHeaders(ctx, path, body, headers)
}

func (c *BeaconClient) postJSONWithHeaders(ctx context.Context, path string, body any, headers http.Header) error {
	url := c.baseURL + path
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(jsonBody)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	maps.Copy(req.Header, headers)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("beacon POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("beacon POST %s: status %d: %s", path, resp.StatusCode, string(respBody))
	}
	return nil
}
