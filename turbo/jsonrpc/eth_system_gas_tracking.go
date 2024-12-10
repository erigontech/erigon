package jsonrpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/log/v3"
)

type L1GasPrice struct {
	timestamp time.Time
	gasPrice  *big.Int
}

type L1GasPriceTracker interface {
	GetLatestPrice() (*big.Int, error)
	GetLowestPrice() (*big.Int, error)
	Start()
	Stop()
}

type RecurringL1GasPriceTracker struct {
	gasLess         bool
	gasPriceFactor  float64
	defaultGasPrice uint64
	maxGasPrice     uint64
	latestPrice     *big.Int
	lowestPrice     *big.Int
	priceHistory    []*big.Int
	rpcUrl          string
	frequency       time.Duration
	totalCount      uint64
	stop            chan struct{}
	latestMtx       *sync.Mutex
	lowestMtx       *sync.Mutex
	running         bool
	lastFetch       time.Time
}

func NewRecurringL1GasPriceTracker(
	gasLess bool,
	gasPriceFactor float64,
	defaultGasPrice uint64,
	maxGasPrice uint64,
	rpcUrl string,
	frequency time.Duration,
	totalCount uint64,
) *RecurringL1GasPriceTracker {
	// ensure we keep at least one historical entry
	if totalCount < 1 {
		totalCount = 1
	}

	return &RecurringL1GasPriceTracker{
		gasLess:         gasLess,
		gasPriceFactor:  gasPriceFactor,
		defaultGasPrice: defaultGasPrice,
		maxGasPrice:     maxGasPrice,
		rpcUrl:          rpcUrl,
		frequency:       frequency,
		stop:            make(chan struct{}),
		latestMtx:       &sync.Mutex{},
		lowestMtx:       &sync.Mutex{},
		totalCount:      totalCount,
	}
}

func (t *RecurringL1GasPriceTracker) setLatestPrice(price *big.Int) {
	t.latestMtx.Lock()
	defer t.latestMtx.Unlock()

	t.latestPrice = price
}

func (t *RecurringL1GasPriceTracker) getLatestPrice() *big.Int {
	t.latestMtx.Lock()
	defer t.latestMtx.Unlock()

	return t.latestPrice
}

func (t *RecurringL1GasPriceTracker) GetLowestPrice() *big.Int {
	t.lowestMtx.Lock()
	defer t.lowestMtx.Unlock()

	return t.lowestPrice
}

func (t *RecurringL1GasPriceTracker) setLowestPrice(price *big.Int) {
	t.lowestMtx.Lock()
	defer t.lowestMtx.Unlock()

	t.lowestPrice = price
}

func (t *RecurringL1GasPriceTracker) GetLatestPrice() (*big.Int, error) {
	if t.gasLess {
		return big.NewInt(0), nil
	}

	// if we're not regularly polling default to old behaviour of just fetching the price
	// once the last one is stale
	if t.frequency == 0 {
		if time.Since(t.lastFetch) > 3*time.Second {
			if err := t.fetchAndStoreNewL1GasPrice(); err != nil {
				return nil, err
			}
		}
		return t.getLatestPrice(), nil
	}

	latest := t.getLatestPrice()
	if latest == nil {
		if err := t.fetchAndStoreNewL1GasPrice(); err != nil {
			return nil, err
		}
		latest = t.getLatestPrice()
	}

	return latest, nil
}

func (t *RecurringL1GasPriceTracker) Start() {
	if t.running || t.frequency == 0 {
		return
	}
	t.running = true
	go func() {
		ticker := time.NewTicker(t.frequency)
		for {
			select {
			case <-t.stop:
				return
			case <-ticker.C:
				log.Trace("[L1GasPriceTracker] Fetching and storing new L1 gas price")
				if err := t.fetchAndStoreNewL1GasPrice(); err != nil {
					log.Error("[L1GasPriceTracker] Failed to fetch and store new L1 gas price", "error", err)
				}
			}
		}
	}()
}

func (t *RecurringL1GasPriceTracker) fetchAndStoreNewL1GasPrice() error {
	latest, err := t.fetchLatestL1Price()
	if err != nil {
		return err
	}
	factored, err := t.applyFactor(latest)
	if err != nil {
		return err
	}
	t.setLatestPrice(factored)
	t.calculateAndStoreNewLowestPrice(factored)
	return nil
}

func (t *RecurringL1GasPriceTracker) Stop() {
	close(t.stop)
	t.running = false
}

func (t *RecurringL1GasPriceTracker) fetchLatestL1Price() (*big.Int, error) {
	res, err := client.JSONRPCCall(t.rpcUrl, "eth_gasPrice")
	if err != nil {
		return nil, err
	}

	if res.Error != nil {
		if strings.Contains(res.Error.Message, t.rpcUrl) {
			replacement := fmt.Sprintf("<%s>", utils.L1RpcUrlFlag.Name)
			res.Error.Message = strings.ReplaceAll(res.Error.Message, t.rpcUrl, replacement)
		}
		return nil, fmt.Errorf("RPC error response: %s", res.Error.Message)
	}

	var resultString string
	if err := json.Unmarshal(res.Result, &resultString); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	price, ok := big.NewInt(0).SetString(resultString[2:], 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert result to big.Int")
	}

	t.lastFetch = time.Now()

	return price, nil
}

func (t *RecurringL1GasPriceTracker) applyFactor(price *big.Int) (*big.Int, error) {
	// Apply factor to calculate l2 gasPrice
	factor := big.NewFloat(0).SetFloat64(t.gasPriceFactor)
	res := new(big.Float).Mul(factor, big.NewFloat(0).SetInt(price))

	// Store l2 gasPrice calculated
	result := new(big.Int)
	res.Int(result)
	minGasPrice := big.NewInt(0).SetUint64(t.defaultGasPrice)
	if minGasPrice.Cmp(result) == 1 { // minGasPrice > result
		result = minGasPrice
	}
	maxGasPrice := new(big.Int).SetUint64(t.maxGasPrice)
	if t.maxGasPrice > 0 && result.Cmp(maxGasPrice) == 1 { // result > maxGasPrice
		result = maxGasPrice
	}

	var truncateValue *big.Int
	log.Debug("Full L2 gas price value: ", result, ". Length: ", len(result.String()))
	numLength := len(result.String())
	if numLength > 3 { //nolint:gomnd
		aux := "%0" + strconv.Itoa(numLength-3) + "d" //nolint:gomnd
		var ok bool
		value := result.String()[:3] + fmt.Sprintf(aux, 0)
		truncateValue, ok = new(big.Int).SetString(value, encoding.Base10)
		if !ok {
			return nil, fmt.Errorf("failed to convert result to big.Int")
		}
	} else {
		truncateValue = result
	}

	if truncateValue == nil {
		return nil, fmt.Errorf("truncateValue nil value detected")
	}

	return truncateValue, nil
}

func (t *RecurringL1GasPriceTracker) calculateAndStoreNewLowestPrice(newPrice *big.Int) {
	t.priceHistory = append(t.priceHistory, newPrice)
	if len(t.priceHistory) > int(t.totalCount) {
		t.priceHistory = t.priceHistory[len(t.priceHistory)-int(t.totalCount):]
	}

	// now figure out the lowest price in all of the history by iterating over the priceHistory
	lowestPrice := t.priceHistory[0]
	for _, price := range t.priceHistory {
		if price.Cmp(lowestPrice) == -1 {
			lowestPrice = price
		}
	}

	t.setLowestPrice(lowestPrice)
}
