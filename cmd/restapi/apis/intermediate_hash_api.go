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

func RegisterIntermediateHashAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/", e.FindIntermediateHash)
	return nil
}

func (e *Env) FindIntermediateHash(c *gin.Context) {
	results, err := findIntermediateHashByPrefix(c.Query("prefix"), e.DB)
	if err != nil {
		c.Error(err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}

type IntermediateHashResponse struct {
	Prefix string `json:"prefix"`
	Value  string `json:"value"`
}

func findIntermediateHashByPrefix(prefixS string, remoteDB ethdb.KV) ([]*IntermediateHashResponse, error) {
	var results []*IntermediateHashResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx ethdb.Tx) error {
		interBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		c := interBucket.Cursor().Prefix(prefix)

		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}

			results = append(results, &IntermediateHashResponse{
				Prefix: fmt.Sprintf("%x\n", k),
				Value:  fmt.Sprintf("%x\n", v),
			})

			if len(results) > 50 {
				results = append(results, &IntermediateHashResponse{
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
