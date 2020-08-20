package apis

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegisterDBAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/buckets-stat", e.BucketsStat)
	router.GET("/size", e.Size)
	return nil
}

func (e *Env) BucketsStat(c *gin.Context) {
	sizes := map[string]map[string]common.StorageSize{}
	for _, name := range dbutils.Buckets {
		sizes[name] = map[string]common.StorageSize{}
	}

	if err := e.KV.View(context.TODO(), func(tx ethdb.Tx) error {
		for _, name := range dbutils.Buckets {
			sz, err := tx.BucketSize(name)
			if err != nil {
				return err
			}
			sizes[name]["size"] = common.StorageSize(sz)
		}
		return nil
	}); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, sizes)
}

func (e *Env) Size(c *gin.Context) {
	results, err := e.DB.(ethdb.HasStats).DiskSize(context.TODO())
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}
