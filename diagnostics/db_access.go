package diagnostics

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/urfave/cli/v2"
)

func SetupDbAccess(ctx *cli.Context) {
	var dataDir string
	if ctx.IsSet("datadir") {
		dataDir = ctx.String("datadir")
	} else {
		dataDir = paths.DataDirForNetwork(paths.DefaultDataDir(), ctx.String("chain"))
	}
	http.HandleFunc("/debug/metrics/db/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbList(w, dataDir)
	})
	http.HandleFunc("/debug/metrics/db/tables", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbTables(w, r, dataDir)
	})
	http.HandleFunc("/debug/metrics/db/read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbRead(w, r, dataDir)
	})
}

func writeDbList(w io.Writer, dataDir string) {
	fmt.Fprintf(w, "SUCCESS\n")
	m := mdbx.PathDbMap()
	for path := range m {
		fmt.Fprintf(w, "%s\n", path)
	}
}

func writeDbTables(w io.Writer, r *http.Request, dataDir string) {
	if err := r.ParseForm(); err != nil {
		fmt.Fprintf(w, "ERROR: parsing arguments: %v\n", err)
		return
	}
	path := r.Form.Get("path")
	if path == "" {
		fmt.Fprintf(w, "ERROR: path argument is required - specify the relative path to an MDBX database directory")
		return
	}
	m := mdbx.PathDbMap()
	db, ok := m[path]
	if !ok {
		fmt.Fprintf(w, "ERROR: path %s is not in the list of allowed paths", path)
		return
	}
	var tables []string
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var e error
		tables, e = tx.ListBuckets()
		if e != nil {
			return e
		}
		return nil
	}); err != nil {
		fmt.Fprintf(w, "ERROR: listing tables in %s: %v\n", path, err)
		return
	}
	fmt.Fprintf(w, "SUCCESS\n")
	for _, table := range tables {
		fmt.Fprintf(w, "%s\n", table)
	}
}

func writeDbRead(w io.Writer, r *http.Request, dataDir string) {

}
