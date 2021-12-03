package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/tx_analysis/utils"
)

var indexTemplate = template.Must(template.New("index").Parse(`
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document</title>
	<script crossorigin src="https://unpkg.com/react@17/umd/react.production.min.js"></script>
	<script crossorigin src="https://unpkg.com/react-dom@17/umd/react-dom.production.min.js"></script>
</head>

<body>
</body>

</html>
`))

var address = "localhost:12345"

type Server struct {
	httpServer *http.Server
	rpcClient  utils.HTTPClient
}

func newServer() *Server {

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)

	httpServer := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	srv := &Server{
		httpServer: httpServer,
		rpcClient:  utils.NewHTTPClient("http://127.0.0.1:8545"),
	}

	mux.HandleFunc("/blocks", srv.handleBlocks)

	return srv
}

func (s *Server) run() {
	go func() {
		fmt.Println("Starting server...")
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
}

func (s *Server) stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	indexTemplate.Execute(w, nil)
}

func (s *Server) handleBlocks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	query, ok := r.URL.Query()["block"]

	if !ok || len(query) < 1 {
		w.Write([]byte("len < 1"))
		// json.NewEncoder(w).Encode()
		return
	}

	blockNstr := query[0]

	blockN, err := strconv.ParseUint(blockNstr, 10, 64)
	if err != nil {
		fmt.Println("ERROROR")
		w.Write([]byte("Error: ERRORROROR"))
	}

	var t rpctest.EthBlockByNumber
	err = utils.GetBlockByNumber(&s.rpcClient, blockN, &t)
	if err != nil {
		fmt.Println("error block by number: ", err)
		return
	}

	if err := json.NewEncoder(w).Encode(&t); err != nil {
		fmt.Println(err)
	}
}

func main() {

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)

	srv := newServer()

	srv.run()

	<-interrupt
	err := srv.stop()
	if err != nil {
		fmt.Println(err)
	}
}
