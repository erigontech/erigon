package apis

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func RegisterPrivateAPI(router *gin.RouterGroup, e *Env) error {
	router.GET("/", e.GetDB)
	router.POST("/", e.PostDB)
	return nil
}

func (e *Env) GetDB(c *gin.Context) {
	var host, port string

	split := strings.Split(e.RemoteDBAddress, ":")
	if len(split) == 2 {
		host, port = split[0], split[1]
	}
	c.JSON(http.StatusOK, map[string]string{"host": host, "port": port})
}

func (e *Env) PostDB(c *gin.Context) {
	newAddr := c.Query("host") + ":" + c.Query("port")
	kv, back, err := ethdb.NewRemote().Path(newAddr).Open("", "", "")
	if err != nil {
		c.Error(err) //nolint:errcheck
		return
	}
	e.RemoteDBAddress = newAddr

	e.KV.Close()

	e.KV = kv
	db := ethdb.NewObjectDatabase(kv)
	e.DB = db
	e.Back = back
	c.Status(http.StatusOK)
}
