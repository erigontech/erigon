package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	url2 "net/url"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

var db *bolt.DB

const (
	bucketName   = "Cache"
	expiryBucket = "Expiry"
)

// methods we don't cache
var methodsToIgnore = map[string]struct{}{}

// methods we configure expiry for
var methodsToExpire = map[string]time.Duration{
	"eth_getBlockByNumber": 1 * time.Minute,
}

// params that trigger expiration
var paramsToExpire = map[string]struct{}{
	"latest":    {},
	"finalized": {},
}

func initDB() {
	var err error
	db, err = bolt.Open("cache.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(expiryBucket))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
}

func fetchFromCache(key string) ([]byte, bool) {
	var data []byte
	var expiry []byte
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		e := tx.Bucket([]byte(expiryBucket))
		data = b.Get([]byte(key))
		expiry = e.Get([]byte(key))
		return nil
	})
	if err != nil || data == nil {
		return nil, false
	}

	if expiry != nil {
		expiryTime, err := time.Parse(time.RFC3339, string(expiry))
		if err == nil && time.Now().After(expiryTime) {
			// Cache entry has expired
			return nil, false
		}
	}

	return data, true
}

func saveToCache(key string, response []byte, duration time.Duration) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		e := tx.Bucket([]byte(expiryBucket))
		err := b.Put([]byte(key), response)
		if err != nil {
			return err
		}
		// Only set expiry if duration is not zero (indicating that it should expire)
		if duration > 0 {
			expiryTime := time.Now().Add(duration).Format(time.RFC3339)
			return e.Put([]byte(key), []byte(expiryTime))
		}
		return nil
	})
	if err != nil {
		log.Println("Failed to save to cache:", err)
	}
}

func generateCacheKey(chainID string, body []byte) (string, error) {
	var request map[string]interface{}
	err := json.Unmarshal(body, &request)
	if err != nil {
		return "", err
	}
	delete(request, "id")
	modifiedBody, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%s", chainID, modifiedBody), nil
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Query().Get("endpoint")
	chainID := r.URL.Query().Get("chainid")
	if endpoint == "" || chainID == "" {
		http.Error(w, "Missing endpoint or chainid parameter", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var request map[string]interface{}
	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, "Invalid JSON-RPC request", http.StatusBadRequest)
		return
	}

	method, ok := request["method"].(string)
	if !ok {
		http.Error(w, "Invalid JSON-RPC method", http.StatusBadRequest)
		return
	}

	cacheKey, err := generateCacheKey(chainID, body)
	if err != nil {
		http.Error(w, "Failed to generate cache key", http.StatusInternalServerError)
		return
	}

	if _, ignore := methodsToIgnore[method]; !ignore {
		if cachedResponse, found := fetchFromCache(cacheKey); found {
			w.Header().Set("Content-Type", "application/json")
			handleWritingResponse(w, request, method, cachedResponse)
			return
		}
	}

	url, _ := url2.Parse(endpoint)
	fmt.Printf("%s Fetching from upstream %s\n", time.Now().Format(time.RFC3339), url.Host)

	resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(body))
	if err != nil {
		http.Error(w, "Failed to fetch from upstream", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Failed to read upstream response", http.StatusInternalServerError)
		return
	}

	if resp.StatusCode == http.StatusOK {
		// Check if the response contains a JSON-RPC error
		var jsonResponse map[string]interface{}
		if err := json.Unmarshal(responseBody, &jsonResponse); err == nil {
			if _, hasError := jsonResponse["error"]; hasError {
				fmt.Println("Received error response from upstream, not caching", url.Host)
			} else {
				if _, ignore := methodsToIgnore[method]; !ignore {
					cacheDuration := time.Duration(0)
					if duration, found := methodsToExpire[method]; found {
						if method == "eth_getBlockByNumber" {
							params, ok := request["params"].([]interface{})
							if ok && len(params) > 0 {
								param, ok := params[0].(string)
								if ok {
									if _, shouldExpire := paramsToExpire[param]; shouldExpire {
										cacheDuration = duration
									}
								}
							}
						} else {
							cacheDuration = duration
						}
					}
					saveToCache(cacheKey, responseBody, cacheDuration)
				}
			}
		} else {
			fmt.Println("Failed to parse upstream response, not caching")
		}
	}

	w.Header().Set("Content-Type", "application/json")
	handleWritingResponse(w, request, method, responseBody)
}

func handleCacheLookup(w http.ResponseWriter, r *http.Request) {
	cacheKey := r.URL.Query().Get("key")
	if cacheKey == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	cachedResponse, found := fetchFromCache(cacheKey)
	if !found {
		http.Error(w, "Cache miss", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(cachedResponse)
}

func handleWritingResponse(w http.ResponseWriter, request map[string]interface{}, method string, response []byte) {
	if chaos > 0 && method == "eth_getLogs" {
		fromNum := request["params"].([]interface{})[0].(map[string]interface{})["fromBlock"].(string)
		if chaosAfter > 0 {
			fromNum = strings.TrimPrefix(fromNum, "0x")
			fromNumInt, err := strconv.ParseUint(fromNum, 16, 64)
			if err == nil && fromNumInt < uint64(chaosAfter) {
				// Skip chaos for blocks before chaosAfter
				fmt.Printf("Skipping chaos for block %d (before %d)\n", fromNumInt, chaosAfter)
				w.Write(response)
				return
			}
		}

		// if we're in chaos mode then roll the dice and remove some random log events
		if rand.Intn(chaos) == 0 {
			fmt.Println("applying chaos to eth_getLogs")
			var jsonResponse map[string]interface{}
			resultsToRemove := []int{}
			if err := json.Unmarshal(response, &jsonResponse); err == nil {
				if result, ok := jsonResponse["result"].([]interface{}); ok {
					if len(result) > 0 {
						// if we have topics or addresses to remove then do that
						if len(logChaosTopics) > 0 || len(logChaosAddresses) > 0 {
							fmt.Println("searching for addresses and topics to remove")
						RESULTS:
							for i, log := range result {
								logMap, ok := log.(map[string]interface{})
								if !ok {
									continue
								}
								address, ok := logMap["address"].(string)
								if !ok {
									continue
								}
								for _, chaosAddress := range logChaosAddresses {
									if address == chaosAddress {
										// remove this log event
										resultsToRemove = append(resultsToRemove, i)
										continue RESULTS
									}
								}

								topics, ok := logMap["topics"].([]interface{})
								if !ok {
									continue
								}
								for _, topic := range topics {
									topicStr, ok := topic.(string)
									if !ok {
										continue
									}
									for _, chaosTopic := range logChaosTopics {
										if topicStr == chaosTopic {
											// remove this log event
											resultsToRemove = append(resultsToRemove, i)
											continue RESULTS
										}
									}
								}
							}
							for i := len(resultsToRemove) - 1; i >= 0; i-- {
								result = append(result[:resultsToRemove[i]], result[resultsToRemove[i]+1:]...)
							}
							jsonResponse["result"] = result
							response, _ = json.Marshal(jsonResponse)
						} else {
							// no topics so just clear down all of the results
							fmt.Println("clearing all results")
							jsonResponse["result"] = []interface{}{}
							response, _ = json.Marshal(jsonResponse)
						}
					}
				}
			}
		}
	}

	w.Write(response)
}

var (
	chaos               = 0
	chaosAfter          = 0
	logChaosFlag        = ""
	logChaosTopics      = []string{}
	logChaosAddressFlag = ""
	logChaosAddresses   = []string{}
)

func main() {
	flag.IntVar(&chaos, "chaos", 0, "Enable chaos mode - some logs will be skipped from responses")
	flag.IntVar(&chaosAfter, "chaos-after", 0, "Enable chaos mode - some logs will be skipped from responses")
	flag.StringVar(&logChaosFlag, "chaos-topics", "", "Topics in csv format to potentially remove in logs chaos mode")
	flag.StringVar(&logChaosAddressFlag, "chaos-addresses", "", "Addresses in csv format to potentially remove in logs chaos mode")
	flag.Parse()

	if len(logChaosFlag) > 0 {
		logChaosTopics = strings.Split(logChaosFlag, ",")
	}

	if len(logChaosAddressFlag) > 0 {
		logChaosAddresses = strings.Split(logChaosAddressFlag, ",")
	}

	initDB()
	defer db.Close()

	http.HandleFunc("/", handleRequest)
	http.HandleFunc("/lookup", handleCacheLookup)
	server := &http.Server{
		Addr:           ":6969",
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Println("Starting proxy server on port 6969")
	log.Println("Chaos frequency:", chaos)
	log.Fatal(server.ListenAndServe())
}
