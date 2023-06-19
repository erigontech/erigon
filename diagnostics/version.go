package diagnostics

import (
	"fmt"
	"io"
	"net/http"

	"github.com/ledgerwatch/erigon/params"
)

const Version = 3

func SetupVersionAccess() {
	http.HandleFunc("/debug/metrics/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeVersion(w)
	})
}

func writeVersion(w io.Writer) {
	fmt.Fprintf(w, "SUCCESS\n")
	fmt.Fprintf(w, "%d\n", Version)
	fmt.Fprintf(w, "%s\n", params.VersionWithMeta)
	fmt.Fprintf(w, "%s\n", params.GitCommit)
}
