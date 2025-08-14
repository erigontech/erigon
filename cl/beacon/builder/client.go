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
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

var _ BuilderClient = &builderClient{}

var (
	ErrNoContent = errors.New("no http content")
)

type builderClient struct {
	// ref: https://ethereum.github.io/builder-specs/#/
	httpClient   *http.Client
	url          *url.URL
	beaconConfig *clparams.BeaconChainConfig
}

func NewBlockBuilderClient(baseUrl string, beaconConfig *clparams.BeaconChainConfig) *builderClient {
	u, err := url.Parse(baseUrl)
	if err != nil {
		panic(err)
	}
	c := &builderClient{
		httpClient:   &http.Client{},
		url:          u,
		beaconConfig: beaconConfig,
	}
	if err := c.GetStatus(context.Background()); err != nil {
		log.Error("cannot connect to builder client", "url", baseUrl, "error", err)
		panic("cannot connect to builder client")
	}
	log.Info("Builder client is ready", "url", baseUrl)
	return c
}

func (b *builderClient) RegisterValidator(ctx context.Context, registers []*cltypes.ValidatorRegistration) error {
	// https://ethereum.github.io/builder-specs/#/Builder/registerValidator
	path := "/eth/v1/builder/validators"
	url := b.url.JoinPath(path).String()
	if len(registers) == 0 {
		return errors.New("empty registers")
	}
	payload, err := json.Marshal(registers)
	if err != nil {
		return err
	}
	_, err = httpCall[json.RawMessage](ctx, b.httpClient, http.MethodPost, url, nil, bytes.NewBuffer(payload), json.RawMessage{})
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
	url := b.url.JoinPath(path).String()
	var headerIn ExecutionHeader
	var epoch uint64
	//
	if b.beaconConfig.SlotsPerEpoch != 0 {
		epoch = uint64(slot / int64(b.beaconConfig.SlotsPerEpoch))
	}
	headerIn.Data = ExecutionHeaderData{Message: ExecutionHeaderMessage{
		Header:             cltypes.NewEth1Header(b.beaconConfig.GetCurrentStateVersion(epoch)),
		ExecutionRequests:  cltypes.NewExecutionRequests(b.beaconConfig),
		BlobKzgCommitments: solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}}

	requestHeader := map[string]string{
		"Date-Milliseconds": strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	header, err := httpCall[ExecutionHeader](ctx, b.httpClient, http.MethodGet, url, requestHeader, nil, headerIn)
	if err != nil {
		log.Warn("[mev builder] httpCall error on GetExecutionPayloadHeader", "err", err, "slot", slot, "parentHash", parentHash.Hex(), "pubKey", pubKey.Hex())
		return nil, err
	}
	return header, nil
}

func (b *builderClient) SubmitBlindedBlocks(ctx context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, *cltypes.ExecutionRequests, error) {
	// https://ethereum.github.io/builder-specs/#/Builder/submitBlindedBlocks
	path := "/eth/v1/builder/blinded_blocks"
	isPostFulu := block.Version().AfterOrEqual(clparams.FuluVersion)
	if isPostFulu {
		path = "/eth/v2/builder/blinded_blocks"
	}
	url := b.url.JoinPath(path).String()
	payload, err := json.Marshal(block)
	if err != nil {
		return nil, nil, nil, err
	}
	headers := map[string]string{
		"Eth-Consensus-Version": block.Version().String(),
	}

	var resp *BlindedBlockResponse

	if isPostFulu {
		_, err = httpCall(ctx, b.httpClient, http.MethodPost, url, headers, bytes.NewBuffer(payload), "")
		if err != nil {
			log.Warn("[mev builder] httpCall error on SubmitBlindedBlocks", "err", err, "slot", block.Block.Slot)
			return nil, nil, nil, err
		}
		return nil, nil, nil, nil // no content expected for Fulu version
	} else {
		resp, err = httpCall(ctx, b.httpClient, http.MethodPost, url, headers, bytes.NewBuffer(payload), BlindedBlockResponse{})
		if err != nil {
			log.Warn("[mev builder] httpCall error on SubmitBlindedBlocks", "err", err, "slot", block.Block.Slot)
			return nil, nil, nil, err
		}
	}

	var eth1Block *cltypes.Eth1Block
	var blobsBundle *engine_types.BlobsBundleV1
	var executionRequests *cltypes.ExecutionRequests
	switch resp.Version {
	case "bellatrix", "capella":
		eth1Block = &cltypes.Eth1Block{}
		if err := json.Unmarshal(resp.Data, block); err != nil {
			return nil, nil, nil, err
		}
	case "deneb":
		denebResp := &struct {
			ExecutionPayload *cltypes.Eth1Block          `json:"execution_payload"`
			BlobsBundle      *engine_types.BlobsBundleV1 `json:"blobs_bundle"`
		}{
			ExecutionPayload: cltypes.NewEth1Block(clparams.DenebVersion, b.beaconConfig),
			BlobsBundle:      &engine_types.BlobsBundleV1{},
		}
		if err := json.Unmarshal(resp.Data, denebResp); err != nil {
			return nil, nil, nil, err
		}
		eth1Block = denebResp.ExecutionPayload
		blobsBundle = denebResp.BlobsBundle
	case "electra", "fulu":
		version, _ := clparams.StringToClVersion(resp.Version)
		denebResp := &struct {
			ExecutionPayload  *cltypes.Eth1Block          `json:"execution_payload"`
			BlobsBundle       *engine_types.BlobsBundleV1 `json:"blobs_bundle"`
			ExecutionRequests *cltypes.ExecutionRequests  `json:"execution_requests"`
		}{
			ExecutionPayload:  cltypes.NewEth1Block(version, b.beaconConfig),
			BlobsBundle:       &engine_types.BlobsBundleV1{},
			ExecutionRequests: cltypes.NewExecutionRequests(b.beaconConfig),
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
	url := b.url.JoinPath(path).String()
	_, err := httpCall[json.RawMessage](ctx, b.httpClient, http.MethodGet, url, nil, nil, json.RawMessage{})
	if errors.Is(err, ErrNoContent) {
		// no content is ok, we just need to check if the server is up
		return nil
	}
	return err
}

func httpCall[T any](ctx context.Context, client *http.Client, method, url string, headers map[string]string, payloadReader io.Reader, body T) (*T, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, payloadReader)
	if err != nil {
		log.Warn("[mev builder] http.NewRequest failed", "err", err, "url", url, "method", method)
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	// send request
	response, err := client.Do(request)
	if err != nil {
		log.Warn("[mev builder] client.Do failed", "err", err, "url", url, "method", method)
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
		bytes, err := io.ReadAll(response.Body)
		if err != nil {
			log.Warn("[mev builder] io.ReadAll failed", "err", err, "url", url, "method", method)
		} else {
			log.Warn("[mev builder] httpCall failed", "status", response.Status, "content", string(bytes))
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
	bytes, err := io.ReadAll(response.Body)
	if err != nil {
		log.Warn("[mev builder] io.ReadAll failed", "err", err, "url", url, "method", method)
		return nil, err
	}
	if len(bytes) == 0 {
		return &body, nil
	}
	if err := json.Unmarshal(bytes, &body); err != nil {
		log.Warn("[mev builder] json.Unmarshal error", "err", err, "content", string(bytes))
		return nil, err
	}
	return &body, nil
}
