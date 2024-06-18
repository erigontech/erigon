package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/boltdb/bolt"
	url2 "net/url"
	"flag"
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

func initDB(file string) {
	var err error
	db, err = bolt.Open(file, 0600, nil)
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

	// Check if the cached response contains an error
	var jsonResponse map[string]interface{}
	if err := json.Unmarshal(data, &jsonResponse); err == nil {
		if _, hasError := jsonResponse["error"]; hasError {
			// Cache entry is an error, evict it
			evictFromCache(key)
			return nil, false
		}
	}

	return data, true
}

func evictFromCache(key string) {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		e := tx.Bucket([]byte(expiryBucket))
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
		return e.Delete([]byte(key))
	})
	if err != nil {
		log.Println("Failed to evict from cache:", err)
	}
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
			w.Header().Set("X-Cache-Status", "HIT")
			w.Write(cachedResponse)
			return
		}
	}

	url, _ := url2.Parse(endpoint)
	fmt.Println("Fetching from upstream", url.Host)

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
	w.Header().Set("X-Cache-Status", "MISS")
	w.Write(responseBody)
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

func main() {
	fileFlag := flag.String("file", "cache.db", "file to read")
	flag.Parse()

	initDB(*fileFlag)
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
	log.Fatal(server.ListenAndServe())
}
