package ui

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/ui/web"
	"github.com/urfave/cli/v2"
)

var Command = cli.Command{
	Name:      "ui",
	Action:    runUI,
	Aliases:   []string{"u"},
	Usage:     "run local ui",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&flags.OutputFlag,
	},
	Description: ``,
}

func runUI(cli *cli.Context) error {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RouteHeaders().
		Route("Origin", "*", cors.Handler(cors.Options{
			AllowedOrigins:   []string{"*"},
			AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowedHeaders:   []string{"Accept", "Content-Type", "session-id"},
			AllowCredentials: false, // <----------<<< do not allow credentials
		})).
		Handler)

	r.Mount("/", web.UI)
	r.HandleFunc("/snapshot-sync", index)
	r.HandleFunc("/sentry-network", index)
	r.HandleFunc("/sentinel-network", index)
	r.HandleFunc("/logs", index)
	r.HandleFunc("/chain", index)
	r.HandleFunc("/data", index)
	r.HandleFunc("/debug", index)
	r.HandleFunc("/testing", index)
	r.HandleFunc("/performance", index)
	r.HandleFunc("/documentation", index)
	r.HandleFunc("/admin", index)
	r.HandleFunc("/downloader", index)

	listenAddr := "127.0.0.1"
	listenPort := 8080

	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", listenAddr, listenPort),
		Handler:           r,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: 1 * time.Minute,
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done() // Signal that the goroutine has completed
		err := srv.ListenAndServe()

		if err != nil {
			log.Fatal(err)
		}
	}()

	wg.Wait() // Wait for the server goroutine to finish
	return nil
}

func index(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "./web/dist/index.html")
}
