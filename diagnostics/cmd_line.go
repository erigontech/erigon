package diagnostics

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func SetupCmdLineAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/debug/metrics/cmdline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeCmdLine(w)
	})
}

func writeCmdLine(w io.Writer) {
	fmt.Fprintf(w, "SUCCESS\n")
	for _, arg := range os.Args {
		fmt.Fprintf(w, "%s\n", arg)
	}
}
