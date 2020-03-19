package apis

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func RegisterRemoteDBAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/", e.GetDB)
	router.POST("/", e.PostDB)
	return nil
}

func (e *Env) GetDB(c *gin.Context) {
	var host, port string
	split := strings.Split(e.DB.GetDialAddr(), ":")
	if len(split) == 2 {
		host, port = split[0], split[1]
	}
	c.JSON(http.StatusOK, map[string]string{"host": host, "port": port})
}

func (e *Env) PostDB(c *gin.Context) {
	newAddr := c.Query("host") + ":" + c.Query("port")
	remoteDB, err := remote.Open(context.Background(), remote.DefaultOpts.Addr(newAddr))
	if err != nil {
		c.Error(err) //nolint:errcheck
		return
	}

	e.DB.Close()
	e.DB = remoteDB
	c.Status(http.StatusOK)
}
