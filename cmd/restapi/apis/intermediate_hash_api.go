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

func RegisterStorageTombstonesAPI(account *gin.RouterGroup, remoteDB *remote.DB) error {
	account.GET("/", func(c *gin.Context) {
		results, err := findIntermediateHashesByPrefix(c.Query("prefix"), remoteDB)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
			return
		}
		c.JSON(http.StatusOK, results)
	})
	return nil
}

type StorageTombsResponse struct {
	Prefix               string `json:"prefix"`
	DontOverlapOtherTomb bool   `json:"dontOverlapOtherTomb"`
	HideStorage          bool   `json:"hideStorage"`
}

func findIntermediateHashesByPrefix(prefixS string, remoteDB *remote.DB) ([]*StorageTombsResponse, error) {
	var results []*StorageTombsResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx *remote.Tx) error {
		interBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		c := interBucket.Cursor(remote.DefaultCursorOpts.PrefetchValues(true))
		cOverlap := interBucket.Cursor(remote.DefaultCursorOpts.PrefetchValues(false).PrefetchSize(1))
		storage := tx.Bucket(dbutils.StorageBucket).Cursor(remote.DefaultCursorOpts.PrefetchValues(false).PrefetchSize(1))

		for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(k, prefix) {
				return nil
			}

			if len(v) > 0 {
				continue
			}

			// 1 prefix must be covered only by 1 tombstone
			overlapK, _, err := cOverlap.Seek(append(k, []byte{0, 0}...))
			if err != nil {
				return err
			}
			overlap := bytes.HasPrefix(overlapK, k)

			// each tomb must cover storage
			storageK, _, err := storage.Seek(k)
			if err != nil {
				return err
			}

			hideStorage := bytes.HasPrefix(storageK, k)

			results = append(results, &StorageTombsResponse{
				Prefix:               fmt.Sprintf("%x\n", k),
				DontOverlapOtherTomb: !overlap,
				HideStorage:          hideStorage,
			})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return results, nil
}
