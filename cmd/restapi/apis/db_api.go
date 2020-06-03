package apis

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegisterDBAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/buckets-stat", e.BucketsStat)
	router.GET("/size", e.Size)
	return nil
}

func (e *Env) BucketsStat(c *gin.Context) {
	results, err := e.DB.(ethdb.HasStats).BucketsStat(context.TODO())
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}

func (e *Env) Size(c *gin.Context) {
	results, err := e.DB.(ethdb.HasStats).DiskSize(context.TODO())
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
		return
	}
	c.JSON(http.StatusOK, results)
}
