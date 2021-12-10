package main

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

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
	}

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
