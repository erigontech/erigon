package ethgasstation

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Init(log.Config{
		Level:   "debug",
		Outputs: []string{"stderr"},
	})
}

func TestGetGasPrice(t *testing.T) {
	ctx := context.Background()
	data := `{"baseFee":10,"blockNumber":15817089,"blockTime":11.88,"gasPrice":{"fast":11,"instant":66,"standard":10},"nextBaseFee":10,"priorityFee":{"fast":2,"instant":2,"standard":1}}`
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, data)
	}))
	defer svr.Close()
	c := NewEthGasStationService()
	c.Url = svr.URL

	gp, err := c.SuggestGasPrice(ctx)
	require.NoError(t, err)
	log.Debug("EthGasStation GasPrice: ", gp)
	assert.Equal(t, big.NewInt(66000000000), gp)
}
