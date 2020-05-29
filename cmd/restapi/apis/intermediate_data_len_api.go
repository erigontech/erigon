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

func RegisterIntermediateDataLenAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/", e.FindIntermediateDataLen)
	return nil
}

func (e *Env) FindIntermediateDataLen(c *gin.Context) {
	results, err := findIntermediateDataLenByPrefix(c.Query("prefix"), e.DB)
	if err != nil {
		c.Error(err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}

type IntermediateDataLenResponse struct {
	Prefix string `json:"prefix"`
	Value  string `json:"value"`
}

func findIntermediateDataLenByPrefix(prefixS string, remoteDB ethdb.KV) ([]*IntermediateDataLenResponse, error) {
	var results []*IntermediateDataLenResponse
	prefix := common.FromHex(prefixS)
	if err := remoteDB.View(context.TODO(), func(tx ethdb.Tx) error {
		interBucket := tx.Bucket(dbutils.IntermediateWitnessSizeBucket)
		c := interBucket.Cursor().Prefix(prefix)

		for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}

			results = append(results, &IntermediateDataLenResponse{
				Prefix: fmt.Sprintf("%x\n", k),
				Value:  fmt.Sprintf("%x\n", v),
			})

			if len(results) > 50 {
				results = append(results, &IntermediateDataLenResponse{
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
