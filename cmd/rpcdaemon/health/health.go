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

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/rpc"
)

type requestBody struct {
	MinPeerCount *uint            `json:"min_peer_count"`
	BlockNumber  *rpc.BlockNumber `json:"known_block"`
}

const (
	urlPath          = "/health"
	healthHeader     = "X-ERIGON-HEALTHCHECK"
	synced           = "synced"
	minPeerCount     = "min_peer_count"
	checkBlock       = "check_block"
	maxSecondsBehind = "max_seconds_behind"
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

	netAPI, ethAPI := parseAPI(rpcAPI)

	headers := r.Header.Values(healthHeader)
	if len(headers) != 0 {
		processFromHeaders(headers, ethAPI, netAPI, w, r)
	} else {
		processFromBody(w, r, netAPI, ethAPI)
	}

	return true
}

func processFromHeaders(headers []string, ethAPI EthAPI, netAPI NetAPI, w http.ResponseWriter, r *http.Request) {
	var (
		errCheckSynced  = errCheckDisabled
		errCheckPeer    = errCheckDisabled
		errCheckBlock   = errCheckDisabled
		errCheckSeconds = errCheckDisabled
	)

	for _, header := range headers {
		lHeader := strings.ToLower(header)
		if lHeader == synced {
			errCheckSynced = checkSynced(ethAPI, r)
		}
		if after, ok := strings.CutPrefix(lHeader, minPeerCount); ok {
			peers, err := strconv.Atoi(after)
			if err != nil {
				errCheckPeer = err
				break
			}
			errCheckPeer = checkMinPeers(uint(peers), netAPI)
		}
		if after, ok := strings.CutPrefix(lHeader, checkBlock); ok {
			block, err := strconv.Atoi(after)
			if err != nil {
				errCheckBlock = err
				break
			}
			errCheckBlock = checkBlockNumber(rpc.BlockNumber(block), ethAPI)
		}
		if after, ok := strings.CutPrefix(lHeader, maxSecondsBehind); ok {
			seconds, err := strconv.Atoi(after)
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
	}

	reportHealthFromHeaders(errCheckSynced, errCheckPeer, errCheckBlock, errCheckSeconds, w)
}

func processFromBody(w http.ResponseWriter, r *http.Request, netAPI NetAPI, ethAPI EthAPI) {
	body, errParse := parseHealthCheckBody(r.Body)
	defer r.Body.Close()

	var errMinPeerCount = errCheckDisabled
	var errCheckBlock = errCheckDisabled

	if errParse != nil {
		log.Root().Warn("unable to process healthcheck request", "err", errParse)
	} else {
		// 1. net_peerCount
		if body.MinPeerCount != nil {
			errMinPeerCount = checkMinPeers(*body.MinPeerCount, netAPI)
		}
		// 2. custom query (shouldn't fail)
		if body.BlockNumber != nil {
			errCheckBlock = checkBlockNumber(*body.BlockNumber, ethAPI)
		}
		// TODO add time from the last sync cycle
	}

	err := reportHealthFromBody(errParse, errMinPeerCount, errCheckBlock, w)
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

func reportHealthFromBody(errParse, errMinPeerCount, errCheckBlock error, w http.ResponseWriter) error {
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

	return writeResponse(w, errors, statusCode)
}

func reportHealthFromHeaders(errCheckSynced, errCheckPeer, errCheckBlock, errCheckSeconds error, w http.ResponseWriter) error {
	statusCode := http.StatusOK
	errs := make(map[string]string)

	if shouldChangeStatusCode(errCheckSynced) {
		statusCode = http.StatusInternalServerError
	}
	errs[synced] = errorStringOrOK(errCheckSynced)

	if shouldChangeStatusCode(errCheckPeer) {
		statusCode = http.StatusInternalServerError
	}
	errs[minPeerCount] = errorStringOrOK(errCheckPeer)

	if shouldChangeStatusCode(errCheckBlock) {
		statusCode = http.StatusInternalServerError
	}
	errs[checkBlock] = errorStringOrOK(errCheckBlock)

	if shouldChangeStatusCode(errCheckSeconds) {
		statusCode = http.StatusInternalServerError
	}
	errs[maxSecondsBehind] = errorStringOrOK(errCheckSeconds)

	return writeResponse(w, errs, statusCode)
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
	if err == nil {
		return "HEALTHY"
	}

	if errors.Is(err, errCheckDisabled) {
		return "DISABLED"
	}

	return fmt.Sprintf("ERROR: %v", err)
}
