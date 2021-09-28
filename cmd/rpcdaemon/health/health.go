package health

import (
	"encoding/json"
	"fmt"
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

	var errMinPeerCount error
	var errMaxTimeLatestSync error
	var errCustomQuery error

	body, errParse := parseHealthCheckBody(r.Body)
	defer r.Body.Close()

	if errParse != nil {
		log.Root().Warn("unable to process healthcheck request", "error", errParse)
		// we return true because we already decided that that is a healthcheck
		// no need to go further
	} else {
		// 1. net_peerCount
		if body.MinPeerCount != nil {
			errMinPeerCount = checkMinPeers(*body.MinPeerCount)
		}

		// 2. time from the last sync cycle (if possible)
		if body.MaxTimeLatestSync != nil {
			errMaxTimeLatestSync = checkMaxTimeLatestSync(*body.MaxTimeLatestSync)
		}

		// 3. custom query (shouldn't fail)
		if len(strings.TrimSpace(body.Query)) > 0 {
			errCustomQuery = checkCustomQuery(body.Query)
		}
	}

	err := reportHealth(errParse, errMinPeerCount, errMaxTimeLatestSync, errCustomQuery, w)
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

func reportHealth(errParse, errMinPeerCount, errMaxTimeLatestSync, errCustomQuery error, w http.ResponseWriter) error {
	statusCode := http.StatusOK
	errors := make(map[string]string)

	if errParse != nil {
		statusCode = http.StatusInternalServerError
	}
	errors["healthcheck_query"] = errorStringOrOK(errParse)

	if errMinPeerCount != nil {
		statusCode = http.StatusInternalServerError
	}
	errors["min_peer_count"] = errorStringOrOK(errMinPeerCount)

	if errMaxTimeLatestSync != nil {
		statusCode = http.StatusInternalServerError
	}
	errors["min_time_latest_sync"] = errorStringOrOK(errMaxTimeLatestSync)

	if errCustomQuery == nil {
		statusCode = http.StatusInternalServerError
	}
	errors["min_custom_query"] = errorStringOrOK(errCustomQuery)

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

func errorStringOrOK(err error) string {
	if err == nil {
		return "HEALTHY"
	}

	return fmt.Sprintf("ERROR: %v", err)
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
