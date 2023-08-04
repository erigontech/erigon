//go:build !noeots

package eth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
	"github.com/otterscan/go-otterscan/ots"
	"github.com/otterscan/go-otterscan/sigs"
	"github.com/otterscan/go-otterscan/topics"
	"github.com/otterscan/go-otterscan/triemap"
	"github.com/vearutop/statigz"
)

func initEmbeddedOts(ctx context.Context, logger log.Logger, httpRpcCfg httpcfg.HttpCfg, apiList []rpc.API) error {
	addr := fmt.Sprintf("%s:%d", httpRpcCfg.EOtsListenAddress, httpRpcCfg.EOtsPort)
	logger.Info("Embedded Otterscan enabled", "on", addr)

	r, srv, err := configureRouter(httpRpcCfg, logger, apiList)
	if err != nil {
		return err
	}

	timeouts := httpRpcCfg.HTTPTimeouts
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadTimeout:       timeouts.ReadTimeout,
		WriteTimeout:      timeouts.WriteTimeout,
		IdleTimeout:       timeouts.IdleTimeout,
		ReadHeaderTimeout: timeouts.ReadTimeout,
	}
	if err := httpSrv.ListenAndServe(); err != nil {
		return err
	}

	defer func() {
		srv.Stop()
	}()

	<-ctx.Done()
	logger.Info("Shutting down embedded Otterscan...")

	return nil
}

func configureRouter(cfg httpcfg.HttpCfg, logger log.Logger, apiList []rpc.API) (chi.Router, *rpc.Server, error) {
	r := chi.NewRouter()
	srv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.RpcStreamingDisable, logger)

	if err := node.RegisterApisFromWhitelist(apiList, nil, srv, false, logger); err != nil {
		return nil, nil, err
	}

	httpHandler := node.NewHTTPHandlerStack(srv, cfg.HttpCORSDomain, cfg.HttpVirtualHost, cfg.HttpCompression)
	r.Group(func(r chi.Router) {
		r.Use(middleware.Recoverer)

		// host the apis
		r.HandleFunc("/signatures/{hash}", triemap.HttpHandler(sigs.Both))
		r.HandleFunc("/topic0/{hash}", triemap.HttpHandler(topics.Both))
		r.Handle("/chains/{chainId}", http.FileServer(http.FS(ots.Chains)))
		r.HandleFunc("/memstats", func(w http.ResponseWriter, r *http.Request) {
			debug.PrintMemStats(false)
		})

		// Serve JSON-RPC under /rpc endpoint
		r.Method("POST", "/rpc", httpHandler)
		r.HandleFunc("/config.json", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{
				"erigonURL":       "/rpc",
				"assetsURLPrefix": "",
			})
		})

		// Serve Otterscan dapp
		fileServer := statigz.FileServer(ots.Dist, statigz.FSPrefix("dist"))
		noCache := middleware.NoCache(fileServer)

		r.Get("/assets/*", func(w http.ResponseWriter, r *http.Request) {
			// Force aggressive 1 year cache (== never expires) to all /assets/* resources;
			// it's safe to aggressive cache them bc file hash is built into filename
			w.Header().Set("Expires", time.Now().Add(time.Hour*24*365).UTC().Format(http.TimeFormat))
			w.Header().Set("Cache-Control", "max-age=31536000")
			http.StripPrefix("/", fileServer).ServeHTTP(w, r)
		})

		// That's necessary to force rootdir to server actual index.html file;
		// also force nocache headers for main website content
		r.Handle("/index.html", http.StripPrefix("/", fileServer))
		r.Handle("/", http.StripPrefix("/", noCache))

		// Fallback everything to /index.html (DON'T redir, fallback and serve same content)
		r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.Path = "/"
			r2.URL.RawPath = "/"
			noCache.ServeHTTP(w, r2)
		})
	})

	return r, srv, nil
}
