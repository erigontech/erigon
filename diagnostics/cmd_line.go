package diagnostics

import (
	"net/http"
	"os"
	"strconv"
	"strings"
)

func SetupCmdLineAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/cmdline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		var space []byte

		w.Write([]byte{'"'})
		for _, arg := range os.Args {
			if len(space) > 0 {
				w.Write(space)
			} else {
				space = []byte(" ")
			}

			w.Write([]byte(strings.Trim(strconv.Quote(arg), `"`)))
		}
		w.Write([]byte{'"'})
	})
}
