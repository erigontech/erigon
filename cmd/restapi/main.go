package main

import (
	"errors"
	"fmt"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()
	root := r.Group("api/v1")

	err := registerAccountAPI(root.Group("account"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("serving on %v... press ctrl+C to abort\n", 8080)
	r.Run()
}

var ErrEntityNotFound = errors.New("entity not found")
