package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

type requestBody struct {
	MinPeerCount      *uint            `json:"min_peer_count"`
	BlockNumber       *rpc.BlockNumber `json:"known_block"`
	SyncTimeThreshold *uint            `json:"sync_time_threshold"`
}

const (
	urlPath = "/health"
)

var (
	errCheckDisabled = errors.New("error check disabled")
)

func ProcessHealthcheckIfNeeded(
	w http.ResponseWriter,
	r *http.Request,
	ctx context.Context,
	rpcAPI []rpc.API,
) bool {
	if !strings.EqualFold(r.URL.Path, urlPath) {
		return false
	}

	netAPI, ethAPI := parseAPI(rpcAPI)

	var errMinPeerCount = errCheckDisabled
	var errCheckBlock = errCheckDisabled
	var errCheckSync = errCheckDisabled

	body, errParse := parseHealthCheckBody(r.Body)
	defer r.Body.Close()

	if errParse != nil {
		log.Root().Warn("unable to process healthcheck request", "error", errParse)
	} else if body.SyncTimeThreshold != nil {
		// 1. net_peerCount
		if body.MinPeerCount != nil {
			errMinPeerCount = checkMinPeers(*body.MinPeerCount, netAPI)
		}
		// 2. custom query (shouldn't fail)
		if body.BlockNumber != nil {
			errCheckBlock = checkBlockNumber(*body.BlockNumber, ethAPI)
		}
		// 3. check time from the last sync cycle
		errCheckSync = checkSyncTimeThreshold(*body.SyncTimeThreshold, ctx, ethAPI)
	}

	err := reportHealth(errParse, errMinPeerCount, errCheckBlock, errCheckSync, w)
	if err != nil {
		log.Root().Warn("unable to process healthcheck request", "error", err)
	}

	return true
}

func parseHealthCheckBody(reader io.Reader) (requestBody, error) {
	var body requestBody

	bodyBytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return body, err
	}

	err = json.Unmarshal(bodyBytes, &body)
	if err != nil {
		return body, err
	}

	return body, nil
}

func reportHealth(errParse, errMinPeerCount, errCheckBlock, errCheckSync error, w http.ResponseWriter) error {
	statusCode := http.StatusOK
	errors := make(map[string]string)

	if shouldChangeStatusCode(errParse) {
		statusCode = http.StatusInternalServerError
	}
	errors["healthcheck_query"] = errorStringOrOK(errParse)

	if shouldChangeStatusCode(errMinPeerCount) {
		statusCode = http.StatusInternalServerError
	}
	errors["min_peer_count"] = errorStringOrOK(errMinPeerCount)

	if shouldChangeStatusCode(errCheckBlock) {
		statusCode = http.StatusInternalServerError
	}
	errors["check_block"] = errorStringOrOK(errCheckBlock)

	if shouldChangeStatusCode(errCheckSync) {
		statusCode = http.StatusInternalServerError
	}
	errors["check_sync"] = errorStringOrOK(errCheckSync)

	w.WriteHeader(statusCode)

	bodyJson, err := json.Marshal(errors)
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
	if err == nil {
		return "HEALTHY"
	}

	if errors.Is(err, errCheckDisabled) {
		return "DISABLED"
	}

	return fmt.Sprintf("ERROR: %v", err)
}
