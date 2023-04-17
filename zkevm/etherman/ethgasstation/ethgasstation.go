package ethgasstation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"

	"github.com/ledgerwatch/erigon/zkevm/encoding"
)

type ethGasStationResponse struct {
	BaseFee     uint64                `json:"baseFee"`
	BlockNumber uint64                `json:"blockNumber"`
	GasPrice    gasPriceEthGasStation `json:"gasPrice"`
}

// gasPriceEthGasStation definition
type gasPriceEthGasStation struct {
	Standard uint64 `json:"standard"`
	Instant  uint64 `json:"instant"`
	Fast     uint64 `json:"fast"`
}

// Client for ethGasStation
type Client struct {
	Http http.Client
	Url  string
}

// NewEthGasStationService is the constructor that creates an ethGasStationService
func NewEthGasStationService() *Client {
	const url = "https://api.ethgasstation.info/api/fee-estimate"
	return &Client{
		Http: http.Client{},
		Url:  url,
	}
}

// SuggestGasPrice retrieves the gas price estimation from ethGasStation
func (e *Client) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	var resBody ethGasStationResponse
	res, err := e.Http.Get(e.Url)
	if err != nil {
		return big.NewInt(0), err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return big.NewInt(0), err
	}
	if res.StatusCode != http.StatusOK {
		return big.NewInt(0), fmt.Errorf("http response is %d", res.StatusCode)
	}
	// Unmarshal result
	err = json.Unmarshal(body, &resBody)
	if err != nil {
		return big.NewInt(0), fmt.Errorf("Reading body failed: %w", err)
	}
	fgp := big.NewInt(0).SetUint64(resBody.GasPrice.Instant)
	return new(big.Int).Mul(fgp, big.NewInt(encoding.Gwei)), nil
}
