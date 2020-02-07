package main

import (
	"errors"
	"fmt"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type, X-Rentals-Auth")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Next()
	})

	root := r.Group("api/v1")

	err := registerAccountAPI(root.Group("accounts"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("serving on %v... press ctrl+C to abort\n", 8080)
	r.Run()
}

var ErrEntityNotFound = errors.New("entity not found")
