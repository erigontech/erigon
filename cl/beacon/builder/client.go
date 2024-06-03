package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

var _ BuilderClient = &builderClient{}

type builderClient struct {
	// ref: https://ethereum.github.io/builder-specs/#/
	httpClient *http.Client
	baseUrl    string
}

func NewBlockBuilderClient(baseUrl string) *builderClient {
	httpC := &http.Client{}
	return &builderClient{
		httpClient: httpC,
		baseUrl:    baseUrl,
	}
}

func (b *builderClient) RegisterValidator(ctx context.Context, registers []*cltypes.ValidatorRegistration) error {
	// https://ethereum.github.io/builder-specs/#/Builder/registerValidator
	url := b.baseUrl + "/eth/v1/builder/validators"
	payload, err := json.Marshal(registers)
	if err != nil {
		return err
	}
	_, err = httpCall[any](ctx, b.httpClient, http.MethodPost, url, nil, bytes.NewBuffer(payload))
	return err
}

func (b *builderClient) GetExecutionPayloadHeader(ctx context.Context, slot int64, parentHash common.Hash, pubKey common.Bytes48) (*ExecutionPayloadHeader, error) {
	// https://ethereum.github.io/builder-specs/#/Builder/getHeader
	url := fmt.Sprintf("%s/eth/v1/builder/header/%d/%s/%s", b.baseUrl, slot, parentHash.Hex(), pubKey.Hex())
	header, err := httpCall[ExecutionPayloadHeader](ctx, b.httpClient, http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (b *builderClient) SubmitBlindedBlocks(ctx context.Context, block *cltypes.SignedBlindedBeaconBlock) (*cltypes.Eth1Block, *engine_types.BlobsBundleV1, error) {
	// https://ethereum.github.io/builder-specs/#/Builder/submitBlindedBlocks
	path := b.baseUrl + "/eth/v1/builder/blinded_blocks"
	payload, err := json.Marshal(block)
	if err != nil {
		return nil, nil, err
	}
	headers := map[string]string{
		"Eth-Consensus-Version": block.Version().String(),
	}
	resp, err := httpCall[BlindedBlockResponse](ctx, b.httpClient, http.MethodPost, path, headers, bytes.NewBuffer(payload))
	if err != nil {
		return nil, nil, err
	}

	var eth1Block *cltypes.Eth1Block
	var blobsBundle *engine_types.BlobsBundleV1
	switch resp.Version {
	case "bellatrix", "capella":
		eth1Block = &cltypes.Eth1Block{}
		if err := json.Unmarshal(resp.Data, block); err != nil {
			return nil, nil, err
		}
	case "deneb":
		denebResp := &struct {
			ExecutionPayload *cltypes.Eth1Block          `json:"execution_payload"`
			BlobsBundle      *engine_types.BlobsBundleV1 `json:"blobs_bundle"`
		}{}
		if err := json.Unmarshal(resp.Data, denebResp); err != nil {
			return nil, nil, err
		}
		eth1Block = denebResp.ExecutionPayload
		blobsBundle = denebResp.BlobsBundle
	}
	return eth1Block, blobsBundle, nil
}

func (b *builderClient) GetStatus(ctx context.Context) error {
	url := b.baseUrl + "/eth/v1/builder/status"
	_, err := httpCall[any](ctx, b.httpClient, http.MethodGet, url, nil, nil)
	return err
}

func httpCall[T any](ctx context.Context, client *http.Client, method, url string, headers map[string]string, payloadReader io.Reader) (*T, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, payloadReader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	// send request
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		// read response body
		var body []byte
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("status code: %d. Response content %v", response.StatusCode, string(body))
	}
	// read response body
	var body T
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return nil, err
	}
	return &body, nil
}
