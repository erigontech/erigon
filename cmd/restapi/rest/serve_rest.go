package rest

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ledgerwatch/turbo-geth/cmd/restapi/apis"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func printError(name string, err error) {
	if err != nil {
		log.Printf("%v: SUCCESS", name)
	} else {
		log.Printf("%v: FAIL (err=%v)", name, err)
	}
}

func ServeREST(ctx context.Context, restHost, rpcHost string, chaindata string) error {
	r := gin.Default()
	root := r.Group("api/v1")
	allowCORS(root)
	root.Use(func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 {
			c.AbortWithStatusJSON(http.StatusInternalServerError, c.Errors)
		}
	})

	var kv ethdb.KV
	var db ethdb.Database
	var back ethdb.Backend
	var err error
	if rpcHost != "" {
		kv, back, err = ethdb.NewRemote().Path(rpcHost).Open("", "", "")
		db = ethdb.NewObjectDatabase(kv)
	} else if chaindata != "" {
		database, errOpen := ethdb.Open(chaindata, true)
		if errOpen != nil {
			return errOpen
		}
		kv = database.KV()
	} else {
		err = fmt.Errorf("either remote or local db must be specified")
	}
	if err != nil {
		return err
	}
	defer db.Close()
	e := &apis.Env{
		KV:              kv,
		DB:              db,
		Back:            back,
		RemoteDBAddress: rpcHost,
		Chaindata:       chaindata,
	}

	if err = apis.RegisterPrivateAPI(root.Group("private-api"), e); err != nil {
		return err
	}
	if err = apis.RegisterAccountAPI(root.Group("accounts"), e); err != nil {
		return err
	}
	if err = apis.RegisterStorageAPI(root.Group("storage"), e); err != nil {
		return err
	}
	if err = apis.RegisterRetraceAPI(root.Group("retrace"), e); err != nil {
		return err
	}
	if err = apis.RegisterDBAPI(root.Group("db"), e); err != nil {
		return err
	}

	log.Printf("serving on %v... press ctrl+C to abort\n", restHost)

	srv := &http.Server{Addr: restHost, Handler: r}

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Fatal("Server forced to shutdown:", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s\n", err)
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
