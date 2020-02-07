package main

import (
	"errors"
	"fmt"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	root := r.Group("api/v1")
	allowCORS(root)

	remoteDbAddress := "localhost:9999"
	remoteDB := MustConnectRemoteDB(remoteDbAddress)
	defer func() {
		fmt.Println("closing remote db")
		if err := remoteDB.Close(); err != nil {
			fmt.Printf("error while closing the remote db: %v\n", err)
		}
	}()

	err := registerAccountAPI(root.Group("accounts"), remoteDB)
	if err != nil {
		panic(err)
	}

	fmt.Printf("serving on %v... press ctrl+C to abort\n", 8080)
	r.Run()
}

var ErrEntityNotFound = errors.New("entity not found")

func allowCORS(r *gin.RouterGroup) {
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Next()
	})
}
