package zkevm

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"bytes"
	"sync/atomic"

	"github.com/holiman/uint256"
	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	ericommon "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/time/rate"
)

func UintBytes(no uint64) []byte {
	noBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(noBytes, no)
	return noBytes
}

func requestWithRetry(ctx context.Context, rpcEndpoint string, payload []byte, maxRetries int) (*common.Hash, error) {
	client := &http.Client{}

	req, err := http.NewRequestWithContext(ctx, "POST", rpcEndpoint, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	var body []byte
	for i := 0; i <= maxRetries; i++ {
		resp, err := client.Do(req)
		if err != nil {
			log.Debug("retrying request for rpc root due to network error", "retryNo", i)
			continue
		}

		func() {
			defer resp.Body.Close()
			body, err = io.ReadAll(resp.Body)
		}()

		if err != nil {
			log.Debug("retrying request for rpc root due to read error", "retryNo", i)
			continue
		}

		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			log.Debug("retrying request for rpc root due to unmarshal error", "retryNo", i)
			continue
		}

		rpcHashString, ok := result["result"].(string)
		if !ok {
			return nil, err
		}

		if rpcHashString == "0x0000000000000000000000000000000000000000000000000000000000000000" {
			continue
		}

		rpcHash := common.HexToHash(rpcHashString)
		return &rpcHash, nil
	}

	return nil, err
}

func getRpcRoot(ctx context.Context, rpcEndpoint string, txNum int64) (common.Hash, error) {
	// int64 to bytes
	txnb := UintBytes(uint64(txNum))
	d1 := ericommon.LeftPadBytes(txnb, 32)
	d2 := ericommon.LeftPadBytes(uint256.NewInt(1).Bytes(), 32)
	mapKey := keccak256.Hash(d1, d2)
	mkh := common.BytesToHash(mapKey)

	payload := map[string]interface{}{
		"method": "eth_getStorageAt",
		"params": []interface{}{
			"0x000000000000000000000000000000005ca1ab1e",
			mkh.Hex(),
			txNum,
		},
		"id":      1,
		"jsonrpc": "2.0",
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return common.Hash{}, err
	}

	rpcHash, err := requestWithRetry(ctx, rpcEndpoint, payloadBytes, 10)
	if err != nil || rpcHash == nil {
		return common.Hash{}, err
	}

	return *rpcHash, nil
}

const (
	numWorkers = 50 // number of concurrent workers
)

func worker(ctx context.Context, logPrefix, rpcEndpoint string, id int, jobs <-chan int64, results chan<- map[int64]string, rl *rate.Limiter, wg *sync.WaitGroup, processedTxNum *int64) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Info(fmt.Sprintf("[%s] Worker exiting due to cancelled context", logPrefix), "id", id)
			return
		case txNum, ok := <-jobs:
			if !ok {
				// jobs channel was closed, exit the goroutine
				return
			}
			if err := rl.Wait(ctx); err != nil {
				log.Error("Error waiting for rate limiter:", "err", err)
				continue
			}
			hash, err := getRpcRoot(ctx, rpcEndpoint, txNum)
			if err != nil {
				log.Error("Error getting hash for txNum", "txNum", txNum, "err", err)
				continue
			}
			results <- map[int64]string{txNum: hash.String()}

			atomic.AddInt64(processedTxNum, 1)
		}
	}
}

func logProcessedTxNumEvery(ctx context.Context, logPrefix string, processedTxNum *int64, totalTxNumToProcess *int64, duration time.Duration, startTime time.Time, doneCh chan struct{}) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-doneCh:
			return
		case <-ticker.C:
			elapsed := time.Since(startTime)
			completed := float64(*processedTxNum)
			total := float64(*totalTxNumToProcess) // de-reference the pointer to get the value

			if completed == 0 {
				log.Info(fmt.Sprintf("[%s] Processed transaction number so far: 0. Total number of transactions to process: ", logPrefix), total, ". Time estimation is not available yet.")
			} else {
				averageTxPerSec := completed / elapsed.Seconds()
				estimatedRemainingTime := time.Duration((total - completed) / averageTxPerSec * float64(time.Second))
				percentageCompleted := (completed / total) * 100 // calculate percentage of completed transactions

				// log.info takes a message and then a set of key value pairs
				log.Info(fmt.Sprintf("[%s] RpcRoot Download Progress", logPrefix), "processed", *processedTxNum, "of", *totalTxNumToProcess, "% complete", percentageCompleted, "time remaining", estimatedRemainingTime)
			}
		}
	}
}

func verifyTxNums(logPrefix string, totalTxNum, startFrom int64, hashResults map[int64]string) []int64 {
	missing := make([]int64, 0)

	for i := startFrom; i <= totalTxNum; i++ {
		if hash, ok := hashResults[i]; !ok || hash == "0x0000000000000000000000000000000000000000000000000000000000000000" {
			missing = append(missing, i)
		}
	}
	log.Info(fmt.Sprintf("[%s] Missing txNums:", logPrefix), "count", len(missing))
	return missing
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please provide a file name")
		os.Exit(1)
	}

	fileName := os.Args[1]

	totalTxNum := 2827625 // edit with highest txnum you want to download hashes up to
	rpcEndpoint := "https://zkevm-rpc.com"
	ctx := context.Background()
	DownloadScalableHashes(ctx, rpcEndpoint, "test", fileName, int64(totalTxNum), true, 1, 250)
}

func DownloadScalableHashes(ctxInput context.Context, rpcEndpoint, logPrefix, fileName string, totalTxNum int64, saveResultsToFile bool, startFrom int64, rateLimit int) map[int64]string {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int64, totalTxNum)
	results := make(chan map[int64]string, totalTxNum)

	rl := rate.NewLimiter(rate.Limit(rateLimit), numWorkers)

	hashResults := make(map[int64]string)
	if saveResultsToFile {
		hashResults = loadExistingData(fileName)
	}
	missingTxNums := verifyTxNums(logPrefix, totalTxNum, startFrom, hashResults)
	missingCount := int64(len(missingTxNums))

	if missingCount == 0 {
		log.Info(fmt.Sprintf("[%s] No missing RPC Roots to download", logPrefix))
		return hashResults
	}

	var doneCh = make(chan struct{})
	var processedTxNum int64
	startTime := time.Now()
	go logProcessedTxNumEvery(ctx, logPrefix, &processedTxNum, &missingCount, 3*time.Second, startTime, doneCh)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, logPrefix, rpcEndpoint, i, jobs, results, rl, &wg, &processedTxNum)
	}

	go func() {
		for _, txNum := range missingTxNums {
			jobs <- txNum
		}
		close(jobs)
	}()

	// handle system signals to gracefully shut down
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for range signals {
			log.Warn(fmt.Sprintf("[%s] Received signal, cancelling context...", logPrefix))
			cancel()
			break
		}
	}()

	go func() {
		wg.Wait()
		close(results)
		close(doneCh)
	}()

	saveTicker := time.NewTicker(60 * time.Second)
	defer saveTicker.Stop()

	for {
		select {
		case res, ok := <-results:
			if ok {
				for k, v := range res {
					hashResults[k] = v
				}
			} else {
				log.Info(fmt.Sprintf("[%s] Results channel was closed - we should be done!", logPrefix))

				if saveResultsToFile {
					saveData(hashResults, fileName)
				}

				log.Info(fmt.Sprintf("[%s] Verifying all transactions...", logPrefix))
				missingTxNums = verifyTxNums(logPrefix, totalTxNum, startFrom, hashResults)
				return hashResults
			}
		case <-saveTicker.C:
			// save data periodically
			if saveResultsToFile {
				saveData(hashResults, fileName)
			}
		}
	}
}

func saveData(results map[int64]string, fileName string) {
	jsonFile, err := os.Create(fileName)
	if err != nil {
		log.Error("Error creating JSON file:", "err", err)
		return
	}
	defer jsonFile.Close()

	jsonWriter := json.NewEncoder(jsonFile)
	jsonWriter.SetIndent("", "    ")
	err = jsonWriter.Encode(results)
	if err != nil {
		log.Error("Error writing JSON data:", "err", err)
	}
}

func loadExistingData(fileName string) map[int64]string {
	file, err := os.Open(fileName)
	if os.IsNotExist(err) {
		return map[int64]string{}
	} else if err != nil {
		log.Error("Error opening JSON file:", "err", err)
		return map[int64]string{}
	}

	defer file.Close()

	var results map[int64]string
	jsonReader := json.NewDecoder(file)
	err = jsonReader.Decode(&results)
	if err != nil {
		log.Error("Error reading JSON data:", "err", err)
		return map[int64]string{}
	}

	return results
}
