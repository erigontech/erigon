package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type http_req struct {
	url    string
	reqID  int
	client *http.Client
}

func new_http_client(url string) http_req {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	return http_req{url: url, client: client}
}

func (h *http_req) post(body string, response interface{}) error {
	r, err := h.client.Post(h.url, "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("status %s", r.Status)
	}

	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(response)
	return err
}

func getBlockByNumber(client *http_req, blockNum uint64, response interface{}) error {
	const template = `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x", true],"id":%d}`
	client.reqID++
	body := fmt.Sprintf(template, blockNum, client.reqID)
	return client.post(body, response)
}
