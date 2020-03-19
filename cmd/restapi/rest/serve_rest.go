package rest

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/cmd/restapi/apis"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func printError(name string, err error) {
	if err != nil {
		log.Printf("%v: SUCCESS", name)
	} else {
		log.Printf("%v: FAIL (err=%v)", name, err)
	}
}

func ServeREST(localAddress, remoteDbAddress string) error {
	r := gin.Default()
	root := r.Group("api/v1")
	allowCORS(root)
	root.Use(func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 {
			c.AbortWithStatusJSON(http.StatusInternalServerError, c.Errors)
		}
	})

	remoteDB, err := remote.Open(context.TODO(), remote.DefaultOpts.Addr(remoteDbAddress))
	if err != nil {
		return err
	}

	defer func() {
		printError("Closing Remote DB", remoteDB.Close())
	}()

	if err = apis.RegisterAccountAPI(root.Group("accounts"), remoteDB); err != nil {
		return err
	}
	if err = apis.RegisterStorageAPI(root.Group("storage"), remoteDB); err != nil {
		return err
	}
	if err = apis.RegisterStorageTombstonesAPI(root.Group("storage-tombstones"), remoteDB); err != nil {
		return err
	}

	log.Printf("serving on %v... press ctrl+C to abort\n", localAddress)

	err = r.Run(localAddress) //nolint:errcheck
	if err != nil {
		return err
	}

	return nil
}

func allowCORS(r *gin.RouterGroup) {
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Next()
	})
}
