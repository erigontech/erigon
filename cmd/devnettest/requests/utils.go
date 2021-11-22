package requests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/log/v3"
)

func post(client *http.Client, url, request string, response interface{}) error {
	fmt.Printf("Request=%s\n", request)
	log.Info("Getting", "url", url, "request", request)
	start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("status %s", r.Status)
	}
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(response)
	log.Info("Got in", "time", time.Since(start).Seconds())
	return err
}