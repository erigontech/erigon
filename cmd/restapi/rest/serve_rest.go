package rest

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/cmd/restapi/apis"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func printError(name string, err error) {
	if err != nil {
		fmt.Printf("%v: SUCCESS", name)
	} else {
		fmt.Printf("%v: FAIL (err=%v)", name, err)
	}
}

func ServeREST(localAddress, remoteDbAddress string) error {
	r := gin.Default()

	root := r.Group("api/v1")
	allowCORS(root)

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

	fmt.Printf("serving on %v... press ctrl+C to abort\n", localAddress)

	r.Run(localAddress) //nolint:errcheck

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
