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

func findIntermediateHashesByPrefix(prefixS string, remoteDB *remote.DB) ([]string, error) {
	var results []string
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx *remote.Tx) error {
		c := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor(remote.DefaultCursorOpts.PrefetchValues(true))

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

			results = append(results, fmt.Sprintf("%x\n", k))
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return results, nil
}
