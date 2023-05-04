package diagnostics

import (
	"fmt"
	"io"
	"net/http"

	"github.com/urfave/cli/v2"
)

func SetupFlagsAccess(ctx *cli.Context) {
	http.HandleFunc("/debug/metrics/flags", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeFlags(w, ctx)
	})
}

func writeFlags(w io.Writer, ctx *cli.Context) {
	fmt.Fprintf(w, "SUCCESS\n")
	for _, flagName := range ctx.FlagNames() {
		fmt.Fprintf(w, "%s=%v\n", flagName, ctx.Value(flagName))
	}
}
