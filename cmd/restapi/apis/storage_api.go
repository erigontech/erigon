package apis

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func RegisterStorageAPI(router *gin.RouterGroup, remoteDB *remote.DB) error {
	router.GET("/", func(c *gin.Context) {
		results, err := findStorageByPrefix(c.Query("prefix"), remoteDB)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
			return
		}
		c.JSON(http.StatusOK, results)
	})
	return nil
}

type StorageResponse struct {
	Prefix string `json:"prefix"`
	Value  string `json:"value"`
}

func findStorageByPrefix(prefixS string, remoteDB *remote.DB) ([]*StorageResponse, error) {
	var results []*StorageResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx *remote.Tx) error {
		c := tx.Bucket(dbutils.StorageBucket).Cursor(remote.DefaultCursorOpts)

		for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(k, prefix) {
				return nil
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
