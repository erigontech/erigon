package apis

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func RegisterStorageAPI(account *gin.RouterGroup, remoteDB *remote.DB) error {
	account.GET("/", func(c *gin.Context) {
		results, err := findStorageTombstoneByPrefix(c.Query("prefix"), remoteDB)
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
			return
		}
		c.JSON(http.StatusOK, results)
	})
	return nil
}
