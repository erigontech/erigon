package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/urfave/cli/v2"
)

func SetupFlagsAccess(ctx *cli.Context, metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/debug/flags", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		flags := map[string]interface{}{}
		for _, flagName := range ctx.FlagNames() {
			flags[flagName] = ctx.Value(flagName)
		}
		json.NewEncoder(w).Encode(flags)
	})
}
