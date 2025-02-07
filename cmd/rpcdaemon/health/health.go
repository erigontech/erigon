package health

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/rpc"
)

type requestBody struct {
	MinPeerCount  *uint            `json:"min_peer_count"`
	BlockNumber   *rpc.BlockNumber `json:"known_block"`
	TxPoolEnable  bool             `json:"tx_pool_enable"`
	LastBlockTime bool             `json:"last_block_time"`
}

const (
	urlPath          = "/health"
	healthHeader     = "X-ERIGON-HEALTHCHECK"
	synced           = "synced"
	minPeerCount     = "min_peer_count"
	checkBlock       = "check_block"
	maxSecondsBehind = "max_seconds_behind"
	txPoolEnable     = "tx_pool_enable"
	lastBlockTime    = "last_block_time"
)

var (
	errCheckDisabled  = errors.New("error check disabled")
	errBadHeaderValue = errors.New("bad header value")
)

func ProcessHealthcheckIfNeeded(
	w http.ResponseWriter,
	r *http.Request,
	rpcAPI []rpc.API,
) bool {
	if !strings.EqualFold(r.URL.Path, urlPath) {
		return false
	}

	netAPI, ethAPI, txPoolAPI := parseAPI(rpcAPI)

	headers := r.Header.Values(healthHeader)
	if len(headers) != 0 {
		processFromHeaders(headers, ethAPI, netAPI, txPoolAPI, w, r)
	} else {
		processFromBody(w, r, netAPI, ethAPI, txPoolAPI)
	}

	return true
}

func processFromHeaders(headers []string, ethAPI EthAPI, netAPI NetAPI, txPoolAPI TxPoolAPI, w http.ResponseWriter, r *http.Request) {
	var (
		errCheckSynced        = errCheckDisabled
		errCheckPeer          = errCheckDisabled
		errCheckBlock         = errCheckDisabled
		errCheckSeconds       = errCheckDisabled
		errCheckTxPool        = errCheckDisabled
		errCheckLastBlockTime = errCheckDisabled
	)
	var blockTime *uint

	for _, header := range headers {
		lHeader := strings.ToLower(header)
		if lHeader == synced {
			errCheckSynced = checkSynced(ethAPI, r)
		}
		if strings.HasPrefix(lHeader, minPeerCount) {
			peers, err := strconv.Atoi(strings.TrimPrefix(lHeader, minPeerCount))
			if err != nil {
				errCheckPeer = err
				break
			}
			errCheckPeer = checkMinPeers(r.Context(), uint(peers), netAPI)
		}
		if strings.HasPrefix(lHeader, checkBlock) {
			block, err := strconv.Atoi(strings.TrimPrefix(lHeader, checkBlock))
			if err != nil {
				errCheckBlock = err
				break
			}
			errCheckBlock = checkBlockNumber(r.Context(), rpc.BlockNumber(block), ethAPI)
		}
		if strings.HasPrefix(lHeader, maxSecondsBehind) {
			seconds, err := strconv.Atoi(strings.TrimPrefix(lHeader, maxSecondsBehind))
			if err != nil {
				errCheckSeconds = err
				break
			}
			if seconds < 0 {
				errCheckSeconds = errBadHeaderValue
				break
			}
			now := time.Now().Unix()
			errCheckSeconds = checkTime(r, int(now)-seconds, ethAPI)
		}
		if lHeader == txPoolEnable {
			errCheckTxPool = checkTxPool(r.Context(), txPoolAPI, ethAPI)
		}
		if lHeader == lastBlockTime {
			blockTime, errCheckLastBlockTime = checkBlockTime(r.Context(), ethAPI)
		}
	}

	reportHealthFromHeaders(errCheckSynced, errCheckPeer, errCheckBlock, errCheckSeconds, errCheckTxPool, errCheckLastBlockTime, blockTime, w)
}

func processFromBody(w http.ResponseWriter, r *http.Request, netAPI NetAPI, ethAPI EthAPI, txPoolAPI TxPoolAPI) {
	body, errParse := parseHealthCheckBody(r.Body)
	defer r.Body.Close()

	var errMinPeerCount = errCheckDisabled
	var errCheckBlock = errCheckDisabled
	var errCheckTxPool = errCheckDisabled
	var errCheckLastBlockTime = errCheckDisabled
	var blockTime *uint

	if errParse != nil {
		log.Root().Warn("unable to process healthcheck request", "err", errParse)
	} else {
		// 1. net_peerCount
		if body.MinPeerCount != nil {
			errMinPeerCount = checkMinPeers(r.Context(), *body.MinPeerCount, netAPI)
		}
		// 2. custom query (shouldn't fail)
		if body.BlockNumber != nil {
			errCheckBlock = checkBlockNumber(r.Context(), *body.BlockNumber, ethAPI)
		}
		// 3. tx_pool pending pool
		if body.TxPoolEnable == true {
			errCheckTxPool = checkTxPool(r.Context(), txPoolAPI, ethAPI)
		}
		// last block time
		if body.LastBlockTime == true {
			blockTime, errCheckLastBlockTime = checkBlockTime(r.Context(), ethAPI)
		}
	}

	err := reportHealthFromBody(errParse, errMinPeerCount, errCheckBlock, errCheckTxPool, errCheckLastBlockTime, blockTime, w)
	if err != nil {
		log.Root().Warn("unable to process healthcheck request", "err", err)
	}
}

func parseHealthCheckBody(reader io.Reader) (requestBody, error) {
	var body requestBody

	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return body, err
	}

	err = json.Unmarshal(bodyBytes, &body)
	if err != nil {
		return body, err
	}

	return body, nil
}

func reportHealthFromBody(errParse, errMinPeerCount, errCheckBlock, errCheckTxPool, errCheckLastBlockTime error, lastBlockTime *uint, w http.ResponseWriter) error {
	statusCode := http.StatusOK
	res := make(map[string]string)

	if shouldChangeStatusCode(errParse) {
		statusCode = http.StatusInternalServerError
	}
	res["healthcheck_query"] = errorStringOrOK(errParse)

	if shouldChangeStatusCode(errMinPeerCount) {
		statusCode = http.StatusInternalServerError
	}
	res["min_peer_count"] = errorStringOrOK(errMinPeerCount)

	if shouldChangeStatusCode(errCheckBlock) {
		statusCode = http.StatusInternalServerError
	}
	res["check_block"] = errorStringOrOK(errCheckBlock)

	if shouldChangeStatusCode(errCheckTxPool) {
		statusCode = http.StatusInternalServerError
	}
	res["tx_pool"] = errorStringOrOK(errCheckTxPool)

	if shouldChangeStatusCode(errCheckLastBlockTime) {
		statusCode = http.StatusInternalServerError
	}
	res["last_block_time"] = blockTimeStringOrError(errCheckLastBlockTime, lastBlockTime)

	return writeResponse(w, res, statusCode)
}

func reportHealthFromHeaders(errCheckSynced, errCheckPeer, errCheckBlock, errCheckSeconds, errCheckTxPool, errCheckLastBlockTime error, lastBlock *uint, w http.ResponseWriter) error {
	statusCode := http.StatusOK
	res := make(map[string]string)

	if shouldChangeStatusCode(errCheckSynced) {
		statusCode = http.StatusInternalServerError
	}
	res[synced] = errorStringOrOK(errCheckSynced)

	if shouldChangeStatusCode(errCheckPeer) {
		statusCode = http.StatusInternalServerError
	}
	res[minPeerCount] = errorStringOrOK(errCheckPeer)

	if shouldChangeStatusCode(errCheckBlock) {
		statusCode = http.StatusInternalServerError
	}
	res[checkBlock] = errorStringOrOK(errCheckBlock)

	if shouldChangeStatusCode(errCheckSeconds) {
		statusCode = http.StatusInternalServerError
	}
	res[maxSecondsBehind] = errorStringOrOK(errCheckSeconds)

	if shouldChangeStatusCode(errCheckTxPool) {
		statusCode = http.StatusInternalServerError
	}
	res[txPoolEnable] = errorStringOrOK(errCheckTxPool)

	if shouldChangeStatusCode(errCheckLastBlockTime) {
		statusCode = http.StatusInternalServerError
	}
	res[lastBlockTime] = blockTimeStringOrError(errCheckLastBlockTime, lastBlock)

	return writeResponse(w, res, statusCode)
}

func writeResponse(w http.ResponseWriter, errs map[string]string, statusCode int) error {
	w.WriteHeader(statusCode)

	bodyJson, err := json.Marshal(errs)
	if err != nil {
		return err
	}

	_, err = w.Write(bodyJson)
	if err != nil {
		return err
	}

	return nil
}

func shouldChangeStatusCode(err error) bool {
	return err != nil && !errors.Is(err, errCheckDisabled)
}

func errorStringOrOK(err error) string {
	if err != nil {
		if errors.Is(err, errCheckDisabled) {
			return "DISABLED"
		}
		return fmt.Sprintf("ERROR: %v", err)
	}

	return "HEALTHY"
}
