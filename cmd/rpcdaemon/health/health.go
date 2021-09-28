package health

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/ledgerwatch/log/v3"
)

type requestBody struct {
	MinPeerCount      *uint  `json:"min_peer_count,omitempty"`
	MaxTimeLatestSync *uint  `json:"max_time_from_latest_sync"`
	Query             string `json:"query"`
}

const HEALTHCHECK_PATH = "/health"

func ProcessHealthcheckIfNeeded(w http.ResponseWriter, r *http.Request) bool {
	if !strings.EqualFold(r.URL.Path, HEALTHCHECK_PATH) {
		return false
	}

	body, err := parseHealthCheckBody(r.Body)
	defer r.Body.Close()

	if err != nil {
		log.Root().Warn("unable to process healthcheck request", "error", err)
		// we return true because we already decided that that is a healthcheck
		// no need to go further
		return true
	}

	// 1. net_peerCount
	var errMinPeerCount error
	if body.MinPeerCount != nil {
		errMinPeerCount = checkMinPeers(*body.MinPeerCount)
	}

	// 2. time from the last sync cycle (if possible)
	var errMaxTimeLatestSync error
	if body.MaxTimeLatestSync != nil {
		errMaxTimeLatestSync = checkMaxTimeLatestSync(*body.MaxTimeLatestSync)
	}

	// 3. custom query (shouldn't fail)
	var errCustomQuery error
	if len(strings.TrimSpace(body.Query)) > 0 {
		errCustomQuery = checkCustomQuery(body.Query)
	}

	err = reportHealth(errMinPeerCount, errMaxTimeLatestSync, errCustomQuery, w)
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

func reportHealth(errMinPeerCount, errMaxTimeLatestSync, errCustomQuery error, w http.ResponseWriter) error {
	statusCode := http.StatusOK

	if errMinPeerCount != nil {
		statusCode = http.StatusInternalServerError
	}

	if errMaxTimeLatestSync != nil {
		statusCode = http.StatusInternalServerError
	}

	if errCustomQuery == nil {
		statusCode = http.StatusInternalServerError
	}

	w.WriteHeader(statusCode)

	return nil
}

func checkMinPeers(minPeers uint) error {
	return nil
}

func checkMaxTimeLatestSync(maxTimeInSeconds uint) error {
	return nil
}

func checkCustomQuery(query string) error {
	return nil
}
