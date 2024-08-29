// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigonwatch"

	"github.com/erigontech/erigon/cmd/diag/flags"
)

var (
	UIURLFlag = cli.StringFlag{
		Name:     "ui.addr",
		Usage:    "URL to serve UI web application",
		Required: false,
		Value:    "127.0.0.1:5137",
	}
)

var Command = cli.Command{
	Name:      "ui",
	Action:    runUI,
	Aliases:   []string{"u"},
	Usage:     "run local ui",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&UIURLFlag,
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

	listenUrl := cli.String(UIURLFlag.Name)

	assets, _ := erigonwatch.UIFiles()
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
	url := "http://" + cli.String(flags.DebugURLFlag.Name)
	addr := DiagAddress{
		Address: url,
	}

	//r.Get("/diagaddr", writeDiagAdderss(addr))
	r.Handle("/data", http.StripPrefix("/data", fs))

	r.HandleFunc("/diagaddr", func(w http.ResponseWriter, r *http.Request) {
		writeDiagAdderss(w, addr)
	})

	srv := &http.Server{
		Addr:              listenUrl,
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

	uiUrl := "http://" + listenUrl
	fmt.Println(text.Hyperlink(uiUrl, "UI running on "+uiUrl))

	wg.Wait() // Wait for the server goroutine to finish
	return nil
}

func addhandler(r *chi.Mux, path string, handler http.Handler) {
	r.Handle(path, http.StripPrefix(path, handler))
}

type DiagAddress struct {
	Address string `json:"address"`
}

func writeDiagAdderss(w http.ResponseWriter, addr DiagAddress) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}
