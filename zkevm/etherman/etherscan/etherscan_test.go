package etherscan

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
	data := `{"status":"1","message":"OK","result":{"LastBlock":"15816910","SafeGasPrice":"10","ProposeGasPrice":"11","FastGasPrice":"55","suggestBaseFee":"9.849758735","gasUsedRatio":"0.779364333333333,0.2434028,0.610012833333333,0.1246597,0.995500566666667"}}`
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, data)
	}))
	defer svr.Close()

	apiKey := ""
	c := NewEtherscanService(apiKey)
	c.config.Url = svr.URL

	gp, err := c.SuggestGasPrice(ctx)
	require.NoError(t, err)
	log.Debug("Etherscan GasPrice: ", gp)
	assert.Equal(t, big.NewInt(55000000000), gp)
}
