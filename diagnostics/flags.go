// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/urfave/cli/v2"
)

func SetupFlagsAccess(ctx *cli.Context, metricsMux *http.ServeMux) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/flags", func(w http.ResponseWriter, r *http.Request) {
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
