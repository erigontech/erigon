package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/urfave/cli/v2"
)

func SetupFlagsAccess(ctx *cli.Context, metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/flags", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		flags := map[string]interface{}{}

		ctxFlags := map[string]struct{}{}

		for _, flagName := range ctx.FlagNames() {
			ctxFlags[flagName] = struct{}{}
		}

		for _, flag := range ctx.App.Flags {
			name := flag.Names()[0]
			value := ctx.Value(name)

			switch typed := value.(type) {
			case string:
				if typed == "" {
					continue
				}
			case cli.UintSlice:
				value = typed.Value()
			}

			var usage string

			if docFlag, ok := flag.(cli.DocGenerationFlag); ok {
				usage = docFlag.GetUsage()
			}

			_, inCtx := ctxFlags[name]

			flags[name] = struct {
				Value   interface{} `json:"value,omitempty"`
				Usage   string      `json:"usage,omitempty"`
				Default bool        `json:"default"`
			}{
				Value:   value,
				Usage:   usage,
				Default: !inCtx,
			}
		}
		json.NewEncoder(w).Encode(flags)
	})
}
