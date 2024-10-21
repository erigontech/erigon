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
	"net/http"
	"os"
	"strconv"
	"strings"
)

func SetupCmdLineAccess(metricsMux *http.ServeMux) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/cmdline", func(w http.ResponseWriter, r *http.Request) {
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
