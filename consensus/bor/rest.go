package bor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/ledgerwatch/log/v3"
)

var (
	stateFetchLimit = 50
)

// ResponseWithHeight defines a response object type that wraps an original
// response with a height.
type ResponseWithHeight struct {
	Height string          `json:"height"`
	Result json.RawMessage `json:"result"`
}

type IHeimdallClient interface {
	Fetch(path string, query string) (*ResponseWithHeight, error)
	FetchWithRetry(path string, query string) (*ResponseWithHeight, error)
	FetchStateSyncEvents(fromID uint64, to int64) ([]*EventRecordWithTime, error)
}

type HeimdallClient struct {
	urlString string
	client    http.Client
}

func NewHeimdallClient(urlString string) (*HeimdallClient, error) {
	h := &HeimdallClient{
		urlString: urlString,
		client: http.Client{
			Timeout: time.Duration(5 * time.Second),
		},
	}
	return h, nil
}

func (h *HeimdallClient) FetchStateSyncEvents(fromID uint64, to int64) ([]*EventRecordWithTime, error) {
	eventRecords := make([]*EventRecordWithTime, 0)
	for {
		queryParams := fmt.Sprintf("from-id=%d&to-time=%d&limit=%d", fromID, to, stateFetchLimit)
		log.Info("Fetching state sync events", "queryParams", queryParams)
		response, err := h.FetchWithRetry("clerk/event-record/list", queryParams)
		if err != nil {
			return nil, err
		}
		var _eventRecords []*EventRecordWithTime
		if response.Result == nil { // status 204
			break
		}
		if err := json.Unmarshal(response.Result, &_eventRecords); err != nil {
			return nil, err
		}
		eventRecords = append(eventRecords, _eventRecords...)
		if len(_eventRecords) < stateFetchLimit {
			break
		}
		fromID += uint64(stateFetchLimit)
	}

	sort.SliceStable(eventRecords, func(i, j int) bool {
		return eventRecords[i].ID < eventRecords[j].ID
	})
	return eventRecords, nil
}

// Fetch fetches response from heimdall
func (h *HeimdallClient) Fetch(rawPath string, rawQuery string) (*ResponseWithHeight, error) {
	u, err := url.Parse(h.urlString)
	if err != nil {
		return nil, err
	}

	u.Path = rawPath
	u.RawQuery = rawQuery

	return h.internalFetch(u)
}

// FetchWithRetry returns data from heimdall with retry
func (h *HeimdallClient) FetchWithRetry(rawPath string, rawQuery string) (*ResponseWithHeight, error) {
	u, err := url.Parse(h.urlString)
	if err != nil {
		return nil, err
	}

	u.Path = rawPath
	u.RawQuery = rawQuery

	for {
		res, err := h.internalFetch(u)
		if err == nil && res != nil {
			return res, nil
		}
		log.Info("Retrying again in 5 seconds for next Heimdall span", "path", u.Path)
		time.Sleep(5 * time.Second)
	}
}

// internal fetch method
func (h *HeimdallClient) internalFetch(u *url.URL) (*ResponseWithHeight, error) {
	res, err := h.client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// check status code
	if res.StatusCode != 200 && res.StatusCode != 204 {
		return nil, fmt.Errorf("Error while fetching data from Heimdall")
	}

	// unmarshall data from buffer
	var response ResponseWithHeight
	if res.StatusCode == 204 {
		return &response, nil
	}

	// get response
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	return &response, nil
}
