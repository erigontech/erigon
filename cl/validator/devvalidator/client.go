package devvalidator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
func (c *BeaconClient) get(ctx context.Context, path string, dst interface{}) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("beacon GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("beacon GET %s: status %d: %s", path, resp.StatusCode, string(body))
	}

	var wrapper beaconResponse
	if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
		return fmt.Errorf("beacon GET %s: decode: %w", path, err)
	}
	return json.Unmarshal(wrapper.Data, dst)
}

// post performs a POST request with a JSON body.
func (c *BeaconClient) post(ctx context.Context, path string, body interface{}) error {
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
func (c *BeaconClient) postAndDecode(ctx context.Context, path string, body interface{}, dst interface{}) error {
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
func (c *BeaconClient) postJSON(ctx context.Context, path string, body interface{}, version string) error {
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
	if version != "" {
		req.Header.Set("Eth-Consensus-Version", version)
	}

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

// postSSZ performs a POST request with an SSZ body and Eth-Consensus-Version header.
func (c *BeaconClient) postSSZ(ctx context.Context, path string, sszBody []byte, version string) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(sszBody)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if version != "" {
		req.Header.Set("Eth-Consensus-Version", version)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("beacon POST SSZ %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("beacon POST SSZ %s: status %d: %s", path, resp.StatusCode, string(body))
	}
	return nil
}
