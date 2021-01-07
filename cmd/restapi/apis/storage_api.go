package apis

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegisterStorageAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/", e.FindStorage)
	return nil
}

func (e *Env) FindStorage(c *gin.Context) {
	results, err := findStorageByPrefix(c.Query("prefix"), e.KV)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}

type StorageResponse struct {
	Prefix string `json:"prefix"`
	Value  string `json:"value"`
}

func findStorageByPrefix(prefixS string, remoteDB ethdb.KV) ([]*StorageResponse, error) {
	var results []*StorageResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HashedStorageBucket).Prefix(prefix).Prefetch(200)

		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if len(k) == 32 {
				continue
			}
			if err != nil {
				return err
			}

			results = append(results, &StorageResponse{
				Prefix: fmt.Sprintf("%x\n", k),
				Value:  fmt.Sprintf("%x\n", v),
			})

			if len(results) > 200 {
				results = append(results, &StorageResponse{
					Prefix: "too much results",
				})
				return nil
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return results, nil
}
