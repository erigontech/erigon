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

package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

var _ BuilderClient = &builderClient{}

var ErrNoContent = errors.New("no http content")

const (
	maxBuilderResponseBodySize = 1 << 20
	maxBuilderErrorBodySize    = 256
	builderPreferencesTimeout  = time.Second
	builderBeaconBlockTimeout  = time.Second
	defaultBuilderCallLimit    = 32
)

type BuilderTargetPolicy struct {
	AllowPrivate bool
}

type builderClient struct {
	// ref: https://ethereum.github.io/builder-specs/#/
	httpClient    *http.Client
	url           *url.URL
	beaconConfig  *clparams.BeaconChainConfig
	lookupIP      func(context.Context, string) ([]net.IPAddr, error)
	targetPolicy  BuilderTargetPolicy
	transport     http.RoundTripper
	admission     *semaphore.Weighted
	admissionOnce sync.Once
}

func NewBlockBuilderClient(baseUrl string, beaconConfig *clparams.BeaconChainConfig) *builderClient {
	return newBlockBuilderClient(baseUrl, beaconConfig, BuilderTargetPolicy{}, true)
}

func NewDynamicBuilderClient(beaconConfig *clparams.BeaconChainConfig, policy BuilderTargetPolicy) *builderClient {
	return newBlockBuilderClient("", beaconConfig, policy, false)
}

func NewBlockBuilderClientWithPolicy(baseURL string, beaconConfig *clparams.BeaconChainConfig, policy BuilderTargetPolicy) *builderClient {
	return newBlockBuilderClient(baseURL, beaconConfig, policy, true)
}

func newBlockBuilderClient(baseUrl string, beaconConfig *clparams.BeaconChainConfig, policy BuilderTargetPolicy, checkStatus bool) *builderClient {
	var u *url.URL
	var err error
	if baseUrl != "" {
		u, err = url.Parse(baseUrl)
		if err != nil {
			panic(err)
		}
	}
	c := &builderClient{
		httpClient:   &http.Client{},
		url:          u,
		beaconConfig: beaconConfig,
		targetPolicy: policy,
		transport:    newPinnedBuilderTransport(nil),
		admission:    semaphore.NewWeighted(defaultBuilderCallLimit),
	}
	if checkStatus {
		if err := c.GetStatus(context.Background()); err != nil {
			log.Error("cannot connect to builder client", "url", baseUrl, "error", err)
			panic("cannot connect to builder client")
		}
		log.Info("Builder client is ready", "url", baseUrl)
	}
	return c
}

func (b *builderClient) RegisterValidator(ctx context.Context, registers []*cltypes.ValidatorRegistration) error {
	// https://ethereum.github.io/builder-specs/#/Builder/registerValidator
	path := "/eth/v1/builder/validators"
	targetURL := b.url.JoinPath(path).String()
	if len(registers) == 0 {
		return errors.New("empty registers")
	}
	payload, err := json.Marshal(registers)
	if err != nil {
		return err
	}
	_, err = httpCall[json.RawMessage](ctx, b.httpClient, http.MethodPost, targetURL, nil, bytes.NewBuffer(payload), json.RawMessage{})
	if errors.Is(err, ErrNoContent) {
		// no content is ok
		return nil
	}
	if err != nil {
		log.Warn("[mev builder] httpCall error on RegisterValidator", "err", err)
	}
	return err
}

func (b *builderClient) GetHeader(ctx context.Context, slot int64, parentHash common.Hash, pubKey common.Bytes48) (*ExecutionHeader, error) {
	// https://ethereum.github.io/builder-specs/#/Builder/getHeader
	path := fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", slot, parentHash.Hex(), pubKey.Hex())
	targetURL := b.url.JoinPath(path).String()
	var headerIn ExecutionHeader
	var epoch uint64
	//
	if b.beaconConfig.SlotsPerEpoch != 0 {
		epoch = uint64(slot / int64(b.beaconConfig.SlotsPerEpoch))
	}
	version := b.beaconConfig.GetCurrentStateVersion(epoch)
	headerIn.Data = ExecutionHeaderData{Message: ExecutionHeaderMessage{
		Header:             cltypes.NewEth1Header(version),
		ExecutionRequests:  cltypes.NewExecutionRequestsWithVersion(b.beaconConfig, version),
		BlobKzgCommitments: solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}}

	requestHeader := map[string]string{
		"Date-Milliseconds": strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	header, err := httpCall[ExecutionHeader](ctx, b.httpClient, http.MethodGet, targetURL, requestHeader, nil, headerIn)
	if err != nil {
		log.Warn("[mev builder] httpCall error on GetExecutionPayloadHeader", "err", err, "slot", slot, "parentHash", parentHash.Hex(), "pubKey", pubKey.Hex())
		return nil, err
	}
	return header, nil
}

func (b *builderClient) SubmitBlindedBlocks(ctx context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *cltypes.ExecutionRequests, error) {
	// https://ethereum.github.io/builder-specs/#/Builder/submitBlindedBlocks
	path := "/eth/v1/builder/blinded_blocks"
	isPostFulu := block.Version().AfterOrEqual(clparams.FuluVersion)
	if isPostFulu {
		path = "/eth/v2/builder/blinded_blocks"
	}
	targetURL := b.url.JoinPath(path).String()
	payload, err := json.Marshal(block)
	if err != nil {
		return nil, nil, nil, err
	}
	headers := map[string]string{
		"Eth-Consensus-Version": block.Version().String(),
	}

	var resp *BlindedBlockResponse

	if isPostFulu {
		_, err = httpCall(ctx, b.httpClient, http.MethodPost, targetURL, headers, bytes.NewBuffer(payload), "")
		if err != nil {
			log.Warn("[mev builder] httpCall error on SubmitBlindedBlocks", "err", err, "slot", block.Block.Slot)
			return nil, nil, nil, err
		}
		return nil, nil, nil, nil // no content expected for Fulu version
	} else {
		resp, err = httpCall(ctx, b.httpClient, http.MethodPost, targetURL, headers, bytes.NewBuffer(payload), BlindedBlockResponse{})
		if err != nil {
			log.Warn("[mev builder] httpCall error on SubmitBlindedBlocks", "err", err, "slot", block.Block.Slot)
			return nil, nil, nil, err
		}
	}

	var eth1Block *cltypes.Eth1Block
	var blobsBundle *engine_types.BlobsBundle
	var executionRequests *cltypes.ExecutionRequests
	switch resp.Version {
	case "bellatrix", "capella":
		eth1Block = &cltypes.Eth1Block{}
		if err := json.Unmarshal(resp.Data, block); err != nil {
			return nil, nil, nil, err
		}
	case "deneb":
		denebResp := &struct {
			ExecutionPayload *cltypes.Eth1Block        `json:"execution_payload"`
			BlobsBundle      *engine_types.BlobsBundle `json:"blobs_bundle"`
		}{
			ExecutionPayload: cltypes.NewEth1Block(clparams.DenebVersion, b.beaconConfig),
			BlobsBundle:      &engine_types.BlobsBundle{},
		}
		if err := json.Unmarshal(resp.Data, denebResp); err != nil {
			return nil, nil, nil, err
		}
		eth1Block = denebResp.ExecutionPayload
		blobsBundle = denebResp.BlobsBundle
	case "electra", "fulu":
		version, _ := clparams.StringToClVersion(resp.Version)
		denebResp := &struct {
			ExecutionPayload  *cltypes.Eth1Block         `json:"execution_payload"`
			BlobsBundle       *engine_types.BlobsBundle  `json:"blobs_bundle"`
			ExecutionRequests *cltypes.ExecutionRequests `json:"execution_requests"`
		}{
			ExecutionPayload:  cltypes.NewEth1Block(version, b.beaconConfig),
			BlobsBundle:       &engine_types.BlobsBundle{},
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(b.beaconConfig, version),
		}
		if err := json.Unmarshal(resp.Data, denebResp); err != nil {
			return nil, nil, nil, err
		}
		eth1Block = denebResp.ExecutionPayload
		blobsBundle = denebResp.BlobsBundle
		executionRequests = denebResp.ExecutionRequests
	}
	return eth1Block, blobsBundle, executionRequests, nil
}

func (b *builderClient) GetStatus(ctx context.Context) error {
	path := "/eth/v1/builder/status"
	targetURL := b.url.JoinPath(path).String()
	_, err := httpCall[json.RawMessage](ctx, b.httpClient, http.MethodGet, targetURL, nil, nil, json.RawMessage{})
	if errors.Is(err, ErrNoContent) {
		// no content is ok, we just need to check if the server is up
		return nil
	}
	return err
}

func (b *builderClient) SubmitBuilderPreferences(ctx context.Context, builderURL string, proposerPubkey common.Bytes48, request *cltypes.BuilderPreferencesRequest) error {
	if request == nil {
		return errors.New("nil builder preferences request")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	requestContext, cancel := context.WithTimeout(ctx, builderPreferencesTimeout)
	defer cancel()
	if err := b.builderAdmission().Acquire(requestContext, 1); err != nil {
		return err
	}
	defer b.builderAdmission().Release(1)
	target, err := b.builderEndpoint(requestContext, b.targetPolicy, builderURL, "eth", "v1", "builder", "builder_preferences", proposerPubkey.Hex())
	if err != nil {
		return err
	}
	response, err := b.builderCall(requestContext, http.MethodPost, target, map[string]string{
		"Eth-Consensus-Version": clparams.GloasVersion.String(),
	}, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	if response.status != http.StatusAccepted {
		return fmt.Errorf("builder preferences: unexpected status code %d", response.status)
	}
	return nil
}

func (b *builderClient) RequestExecutionPayloadBid(ctx context.Context, builderURL string, slot uint64, parentHash, parentRoot common.Hash, proposerPubkey common.Bytes48, auth *cltypes.SignedBuilderRequestAuth, timeout time.Duration) (*cltypes.SignedExecutionPayloadBid, error) {
	if auth == nil {
		return nil, errors.New("nil builder request auth")
	}
	timeoutMilliseconds := timeout.Milliseconds()
	if timeoutMilliseconds <= 0 {
		return nil, errors.New("builder request timeout must be at least one millisecond")
	}
	requestContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := b.builderAdmission().Acquire(requestContext, 1); err != nil {
		return nil, err
	}
	defer b.builderAdmission().Release(1)
	payload, err := json.Marshal(auth)
	if err != nil {
		return nil, err
	}
	target, err := b.builderEndpoint(requestContext, b.targetPolicy, builderURL, "eth", "v1", "builder", "execution_payload_bid", strconv.FormatUint(slot, 10), parentHash.Hex(), parentRoot.Hex(), proposerPubkey.Hex())
	if err != nil {
		return nil, err
	}
	response, err := b.builderCall(requestContext, http.MethodPost, target, map[string]string{
		"Eth-Consensus-Version": clparams.GloasVersion.String(),
		"Date-Milliseconds":     strconv.FormatInt(time.Now().UnixMilli(), 10),
		"X-Timeout-Ms":          strconv.FormatInt(timeoutMilliseconds, 10),
	}, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if response.status == http.StatusNoContent {
		return nil, nil
	}
	if response.status != http.StatusOK {
		return nil, fmt.Errorf("execution payload bid: unexpected status code %d", response.status)
	}
	if response.header.Get("Eth-Consensus-Version") != clparams.GloasVersion.String() {
		return nil, errors.New("execution payload bid response is missing the Gloas consensus version")
	}
	var responseBody struct {
		Version string                             `json:"version"`
		Data    *cltypes.SignedExecutionPayloadBid `json:"data"`
	}
	if err := json.Unmarshal(response.body, &responseBody); err != nil {
		return nil, fmt.Errorf("decode execution payload bid: %w", err)
	}
	if responseBody.Version != clparams.GloasVersion.String() || responseBody.Data == nil || responseBody.Data.Message == nil {
		return nil, errors.New("execution payload bid response is missing Gloas data")
	}
	return responseBody.Data, nil
}

func (b *builderClient) SubmitSignedBeaconBlock(ctx context.Context, builderURL string, block *cltypes.SignedBeaconBlock) error {
	if block == nil || block.Block == nil || block.Block.Body == nil {
		return errors.New("nil signed beacon block")
	}
	payload, err := json.Marshal(block)
	if err != nil {
		return err
	}
	requestContext, cancel := context.WithTimeout(ctx, builderBeaconBlockTimeout)
	defer cancel()
	if err := b.builderAdmission().Acquire(requestContext, 1); err != nil {
		return err
	}
	defer b.builderAdmission().Release(1)
	target, err := b.builderEndpoint(requestContext, b.targetPolicy, builderURL, "eth", "v1", "builder", "beacon_blocks")
	if err != nil {
		return err
	}
	response, err := b.builderCall(requestContext, http.MethodPost, target, map[string]string{
		"Eth-Consensus-Version": block.Version().String(),
	}, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	if response.status != http.StatusAccepted {
		return fmt.Errorf("signed beacon block: unexpected status code %d", response.status)
	}
	return nil
}

type builderTarget struct {
	url      string
	hostname string
	ips      []net.IP
}

func (b *builderClient) builderEndpoint(ctx context.Context, policy BuilderTargetPolicy, rawURL string, path ...string) (builderTarget, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return builderTarget{}, err
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil {
		return builderTarget{}, errors.New("builder URL must be an HTTP(S) URL without user information")
	}
	hostname := target.Hostname()
	addresses := []net.IPAddr(nil)
	if ip := net.ParseIP(hostname); ip != nil {
		addresses = []net.IPAddr{{IP: ip}}
	} else {
		lookup := b.lookupIP
		if lookup == nil {
			lookup = net.DefaultResolver.LookupIPAddr
		}
		addresses, err = lookup(ctx, hostname)
		if err != nil {
			return builderTarget{}, fmt.Errorf("resolve builder URL: %w", err)
		}
	}
	if len(addresses) == 0 {
		return builderTarget{}, errors.New("builder URL has no resolved addresses")
	}
	for _, address := range addresses {
		if !isAllowedBuilderIP(address.IP, policy) {
			return builderTarget{}, fmt.Errorf("builder URL resolves to disallowed address %s", address.IP)
		}
	}
	validatedIPs := make([]net.IP, len(addresses))
	for i, address := range addresses {
		validatedIPs[i] = append(net.IP(nil), address.IP...)
	}
	return builderTarget{url: target.JoinPath(path...).String(), hostname: hostname, ips: validatedIPs}, nil
}

func isPublicBuilderIP(ip net.IP) bool {
	return ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsUnspecified()
}

func isAllowedBuilderIP(ip net.IP, policy BuilderTargetPolicy) bool {
	if isPublicBuilderIP(ip) {
		return true
	}
	return policy.AllowPrivate && ip != nil && (ip.IsPrivate() || ip.IsLoopback())
}

type builderHTTPResponse struct {
	body   []byte
	header http.Header
	status int
}

func (b *builderClient) builderCall(ctx context.Context, method string, target builderTarget, headers map[string]string, body io.Reader) (*builderHTTPResponse, error) {
	var attemptTimeout time.Duration
	if deadline, ok := ctx.Deadline(); ok && len(target.ips) > 1 {
		attemptTimeout = time.Until(deadline) / time.Duration(len(target.ips))
	} else if len(target.ips) > 1 {
		attemptTimeout = time.Second
	}
	requestContext := context.WithValue(ctx, pinnedBuilderTargetKey{}, pinnedBuilderTarget{hostname: target.hostname, ips: target.ips, attemptTimeout: attemptTimeout})
	request, err := http.NewRequestWithContext(requestContext, method, target.url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	if b.httpClient == nil {
		return nil, errors.New("nil builder HTTP client")
	}
	client := *b.httpClient
	if b.transport != nil {
		client.Transport = b.transport
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("builder redirects are not allowed")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	bodyLimit := int64(maxBuilderResponseBodySize)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		bodyLimit = maxBuilderErrorBodySize
	}
	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, bodyLimit+1))
	if err != nil {
		return nil, err
	}
	if len(bodyBytes) > maxBuilderResponseBodySize && bodyLimit == maxBuilderResponseBodySize {
		return nil, fmt.Errorf("builder response exceeds %d bytes", maxBuilderResponseBodySize)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("builder returned status code %d", response.StatusCode)
	}
	return &builderHTTPResponse{body: bodyBytes, header: response.Header.Clone(), status: response.StatusCode}, nil
}

func (b *builderClient) builderAdmission() *semaphore.Weighted {
	b.admissionOnce.Do(func() {
		if b.admission == nil {
			b.admission = semaphore.NewWeighted(defaultBuilderCallLimit)
		}
	})
	return b.admission
}

type pinnedBuilderTargetKey struct{}

type pinnedBuilderTarget struct {
	hostname       string
	ips            []net.IP
	attemptTimeout time.Duration
}

func newPinnedBuilderTransport(dialContext func(context.Context, string, string) (net.Conn, error)) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if dialContext == nil {
		dialContext = (&net.Dialer{}).DialContext
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		pinned, ok := ctx.Value(pinnedBuilderTargetKey{}).(pinnedBuilderTarget)
		if !ok {
			return nil, errors.New("builder target was not resolved before dialing")
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(host, pinned.hostname) {
			return nil, errors.New("builder dial target does not match resolved host")
		}
		var dialErrors []error
		for i, ip := range pinned.ips {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			pinnedAddress := net.JoinHostPort(ip.String(), port)
			attemptCtx := ctx
			cancel := func() {}
			if i+1 < len(pinned.ips) && pinned.attemptTimeout > 0 {
				attemptCtx, cancel = context.WithTimeout(ctx, pinned.attemptTimeout)
			}
			conn, err := dialContext(attemptCtx, network, pinnedAddress)
			cancel()
			if err == nil {
				return conn, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			dialErrors = append(dialErrors, fmt.Errorf("dial builder address %s: %w", pinnedAddress, err))
		}
		return nil, errors.Join(dialErrors...)
	}
	return transport
}

func httpCall[T any](ctx context.Context, client *http.Client, method, rawURL string, headers map[string]string, payloadReader io.Reader, body T) (*T, error) {
	request, err := http.NewRequestWithContext(ctx, method, rawURL, payloadReader)
	if err != nil {
		log.Warn("[mev builder] http.NewRequest failed", "err", err, "url", rawURL, "method", method)
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	// send request
	response, err := client.Do(request)
	if err != nil {
		log.Warn("[mev builder] client.Do failed", "err", err, "url", rawURL, "method", method)
		return nil, err
	}
	defer func() {
		if response.Body != nil {
			response.Body.Close()
		}
	}()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		// read response body
		if response.Body == nil {
			return nil, fmt.Errorf("status code: %d", response.StatusCode)
		}
		bodyBytes, err := io.ReadAll(response.Body)
		if err != nil {
			log.Warn("[mev builder] io.ReadAll failed", "err", err, "url", rawURL, "method", method)
		} else {
			log.Warn("[mev builder] httpCall failed", "status", response.Status, "content", string(bodyBytes))
		}
		return nil, fmt.Errorf("status code: %d", response.StatusCode)
	}
	if response.StatusCode == http.StatusNoContent {
		return nil, ErrNoContent
	}

	// read response body
	if response.Body == nil {
		return &body, nil
	}
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		log.Warn("[mev builder] io.ReadAll failed", "err", err, "url", rawURL, "method", method)
		return nil, err
	}
	if len(bodyBytes) == 0 {
		return &body, nil
	}
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		log.Warn("[mev builder] json.Unmarshal error", "err", err, "content", string(bodyBytes))
		return nil, err
	}
	return &body, nil
}
