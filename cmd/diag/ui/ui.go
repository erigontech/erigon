package ui

import (
	"encoding/json"
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
	supportedSubpaths := []string{
		"sentry-network",
		"sentinel-network",
		"downloader",
		"logs",
		"chain",
		"data",
		"debug",
		"testing",
		"performance",
		"documentation",
		"issues",
		"admin",
	}

	listenAddr := "127.0.0.1"
	listenPort := 8080

	assets, _ := web.Assets()
	fs := http.FileServer(http.FS(assets))

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

	r.Mount("/", fs)

	for _, subpath := range supportedSubpaths {
		addhandler(r, "/"+subpath, fs)
	}

	// Use the file system to serve static files

	r.Get("/diagaddr", writeDiagAdderss)
	r.Handle("/data", http.StripPrefix("/data", fs))

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

func addhandler(r *chi.Mux, path string, handler http.Handler) {
	r.Handle(path, http.StripPrefix(path, handler))
}

type DiagAddress struct {
	Address string `json:"address"`
}

func writeDiagAdderss(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	addr := DiagAddress{
		Address: "http://127.0.0.1:6060",
	}

	if err := json.NewEncoder(w).Encode(addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}
