package diagnostics

import (
	"fmt"
	"net/http"
	"os"
)

func SetupCmdLineAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/debug/cmdline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		space := ""
		fmt.Fprint(w, '"')
		for _, arg := range os.Args {
			if len(space) > 0 {
				fmt.Fprint(w, space)
			} else {
				space = " "
			}

			fmt.Fprint(w, arg)
		}
		fmt.Fprint(w, '"')
	})
}
