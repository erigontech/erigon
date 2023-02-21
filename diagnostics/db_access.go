package diagnostics

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon/common/paths"
	turbocli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/urfave/cli/v2"
)

func SetupDbAccess(ctx *cli.Context) {
	var dataDir string
	if ctx.IsSet(turbocli.DataDirFlag.Name) {
		dataDir = ctx.String(turbocli.DataDirFlag.Name)
	} else {
		dataDir = turbocli.DataDirForNetwork(paths.DefaultDataDir(), ctx.String(turbocli.ChainFlag.Name))
	}
	http.HandleFunc("/debug/metrics/db/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbList(w, dataDir)
	})
	http.HandleFunc("/debug/metrics/db/read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbRead(w, r, dataDir)
	})
}

func writeDbList(w io.Writer, dataDir string) {
	// Search for mdbx databases in the directory tree dataDir
	dirs := []string{""}
	results := []string{}
	for len(dirs) == 0 {
		dir := dirs[len(dirs)-1]
		dirs = dirs[:len(dirs)-1]
		entries, err := os.ReadDir(filepath.Join(dataDir, dir))
		if err != nil {
			fmt.Fprintf(w, "ERROR: read directory %s: %v\n", dir, err)
			return
		}
		for _, entry := range entries {
			if entry.IsDir() {
				dirs = append(dirs, filepath.Join(dir, entry.Name()))
			} else if entry.Name() == "mdbx.dat" {
				results = append(results, dir)
			}
		}
	}
	fmt.Fprintf(w, "SUCCESS\n")
	for _, result := range results {
		fmt.Fprintf(w, "%s\n", result)
	}
}

func writeDbRead(w io.Writer, r *http.Request, dataDir string) {

}
