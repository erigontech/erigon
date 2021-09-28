package health

import (
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
	MaxTimeLatestSync *uint            `json:"max_time_from_latest_sync"`
	BlockNumber       *rpc.BlockNumber `json:"known_block"`
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
	rpcAPI []rpc.API,
) bool {
	if !strings.EqualFold(r.URL.Path, urlPath) {
		return false
	}

	netAPI, ethAPI := parseAPI(rpc.API)

	var errMinPeerCount = errCheckDisabled
	var errMaxTimeLatestSync = errCheckDisabled
	var errCheckBlock = errCheckDisabled

	body, errParse := parseHealthCheckBody(r.Body)
	defer r.Body.Close()

	if errParse != nil {
		log.Root().Warn("unable to process healthcheck request", "error", errParse)
		// we return true because we already decided that that is a healthcheck
		// no need to go further
	} else {
		// 1. net_peerCount
		if body.MinPeerCount != nil {
			errMinPeerCount = checkMinPeers(*body.MinPeerCount, netAPI)
		}
		// 2. time from the last sync cycle (if possible)
		if body.MaxTimeLatestSync != nil {
			errMaxTimeLatestSync = checkMaxTimeLatestSync(*body.MaxTimeLatestSync)
		}
		// 3. custom query (shouldn't fail)
		if body.BlockNumber != nil {
			errCheckBlock = checkBlockNumber(*body.BlockNumber, ethAPI)
		}
	}

	err := reportHealth(errParse, errMinPeerCount, errMaxTimeLatestSync, errCheckBlock, w)
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

func reportHealth(errParse, errMinPeerCount, errMaxTimeLatestSync, errCheckBlock error, w http.ResponseWriter) error {
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

	if shouldChangeStatusCode(errMaxTimeLatestSync) {
		statusCode = http.StatusInternalServerError
	}
	errors["min_time_latest_sync"] = errorStringOrOK(errMaxTimeLatestSync)

	if shouldChangeStatusCode(errCheckBlock) {
		statusCode = http.StatusInternalServerError
	}
	errors["check_block"] = errorStringOrOK(errCheckBlock)

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

func checkMaxTimeLatestSync(maxTimeInSeconds uint) error {
	return nil
}
