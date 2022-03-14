package requests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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

func HexToInt(hexStr string) uint64 {
	// Remove the 0x prefix
	cleaned := strings.ReplaceAll(hexStr, "0x", "")

	result, _ := strconv.ParseUint(cleaned, 16, 64)
	return result
}
