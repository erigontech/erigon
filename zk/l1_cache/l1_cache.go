package l1_cache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"errors"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

const (
	bucketName   = "Cache"
	expiryBucket = "Expiry"
)

// methods we don't cache
var methodsToIgnore = map[string]struct{}{}

// methods we configure expiry for
var methodsToExpire = map[string]time.Duration{
	//"eth_getBlockByNumber": 1 * time.Minute, // example here but currently we don't want any methods to expire
}

// params that trigger expiration
var paramsToExpire = map[string]struct{}{
	"latest":    {},
	"finalized": {},
}

type L1Cache struct {
	server *http.Server
	db     kv.RwDB
}

func NewL1Cache(ctx context.Context, dbPath string, port uint) (*L1Cache, error) {
	db := mdbx.NewMDBX(log.New()).Path(dbPath).MustOpen()

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := tx.CreateBucket(bucketName); err != nil {
		return nil, err
	}
	if err := tx.CreateBucket(expiryBucket); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	http.HandleFunc("/", handleRequest(db))
	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr:           addr,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		log.Info("Starting L1 Cache Server on port:", "port", port)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("L1 Cache Server stopped", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		log.Info("Shutting down L1 Cache Server...")
		if err := server.Shutdown(context.Background()); err != nil {
			log.Error("Failed to shutdown L1 Cache Server", "error", err)
		}
		db.Close()
	}()

	return &L1Cache{
		server: server,
		db:     db,
	}, nil
}

func fetchFromCache(tx kv.RwTx, key string) ([]byte, bool) {
	data, err := tx.GetOne(bucketName, []byte(key))
	if err != nil || data == nil {
		return nil, false
	}

	expiry, err := tx.GetOne(expiryBucket, []byte(key))
	if err == nil && expiry != nil {
		expiryTime, err := time.Parse(time.RFC3339, string(expiry))
		if err == nil && time.Now().After(expiryTime) {
			// Cache entry has expired
			evictFromCache(tx, key)
			return nil, false
		}
	}

	// Check if the cached response contains an error
	var jsonResponse map[string]interface{}
	if err := json.Unmarshal(data, &jsonResponse); err == nil {
		if _, hasError := jsonResponse["error"]; hasError {
			// Cache entry is an error, evict it
			evictFromCache(tx, key)
			return nil, false
		}
	}

	return data, true
}

func evictFromCache(tx kv.RwTx, key string) {
	if err := tx.Delete(bucketName, []byte(key)); err != nil {
		log.Warn("Failed to evict from cache", "error", err)
	}
	if err := tx.Delete(expiryBucket, []byte(key)); err != nil {
		log.Warn("Failed to evict from cache", "error", err)
	}
}

func saveToCache(tx kv.RwTx, key string, response []byte, duration time.Duration) error {
	if err := tx.Put(bucketName, []byte(key), response); err != nil {
		return err
	}
	// Only set expiry if duration is not zero (indicating that it should expire)
	if duration > 0 {
		expiryTime := time.Now().Add(duration).Format(time.RFC3339)
		if err := tx.Put(expiryBucket, []byte(key), []byte(expiryTime)); err != nil {
			return err
		}
	}
	return nil
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

func handleRequest(db kv.RwDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			tx, err := db.BeginRw(r.Context())
			if err != nil {
				http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
				return
			}
			if cachedResponse, found := fetchFromCache(tx, cacheKey); found {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache-Status", "HIT")
				w.Write(cachedResponse)
				tx.Commit()
				return
			}
			tx.Rollback()
		}

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
					log.Warn("Received error response from upstream, not caching", "error", jsonResponse["error"])
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
						tx, err := db.BeginRw(r.Context())
						if err != nil {
							http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
							return
						}
						defer tx.Rollback()
						if err := saveToCache(tx, cacheKey, responseBody, cacheDuration); err != nil {
							http.Error(w, "Failed to save to cache", http.StatusInternalServerError)
							return
						}
						tx.Commit()
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
}
