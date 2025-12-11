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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/node/paths"
)

func SetupDbAccess(ctx *cli.Context, metricsMux *http.ServeMux) {
	if metricsMux == nil {
		return
	}

	var dataDir string
	if ctx.IsSet("datadir") {
		dataDir = ctx.String("datadir")
	} else {
		dataDir = paths.DataDirForNetwork(paths.DefaultDataDir(), ctx.String("chain"))
	}
	metricsMux.HandleFunc("/dbs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeDbList(w, dataDir)
	})
	metricsMux.HandleFunc("/dbs/", func(w http.ResponseWriter, r *http.Request) {

		urlPath := r.URL.Path

		if !strings.HasPrefix(urlPath, "/dbs/") {
			http.Error(w, fmt.Sprintf(`Unexpected path prefix: expected: "/dbs/..." got: "%s"`, urlPath), http.StatusNotFound)
			return
		}

		pathParts := strings.Split(urlPath[5:], "/")

		if len(pathParts) < 1 {
			http.Error(w, fmt.Sprintf(`Unexpected path len: expected: "{db}/tables" got: "%s"`, urlPath), http.StatusNotFound)
			return
		}

		var dbname string
		var sep string

		for len(pathParts) > 0 {
			dbname += sep + pathParts[0]

			if sep == "" {
				sep = "/"
			}

			pathParts = pathParts[1:]

			if pathParts[0] == "tables" {
				break
			}

			if len(pathParts) < 2 {
				http.Error(w, fmt.Sprintf(`Unexpected path part: expected: "tables" got: "%s"`, pathParts[0]), http.StatusNotFound)
				return
			}
		}

		switch len(pathParts) {
		case 1:
			writeDbTables(w, r, dataDir, dbname)
		case 2:
			offset, err := offsetValue(r.URL.Query())

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			limit, err := limitValue(r.URL.Query(), 0)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			writeDbRead(w, r, dataDir, dbname, pathParts[1], nil, offset, limit)
		case 3:
			key, err := base64.URLEncoding.DecodeString(pathParts[2])

			if err != nil {
				http.Error(w, fmt.Sprintf(`key "%s" argument should be base64url encoded: %v`, pathParts[2], err), http.StatusBadRequest)
				return
			}

			offset, err := offsetValue(r.URL.Query())

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			limit, err := limitValue(r.URL.Query(), 0)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			writeDbRead(w, r, dataDir, dbname, pathParts[1], key, offset, limit)

		default:
			http.Error(w, fmt.Sprintf(`Unexpected path parts: "%s"`, strings.Join(pathParts[2:], "/")), http.StatusNotFound)
		}
	})
}

func writeDbList(w http.ResponseWriter, dataDir string) {
	w.Header().Set("Content-Type", "application/json")
	m := mdbx.PathDbMap()
	dbs := make([]string, 0, len(m))
	for path := range m {
		dbs = append(dbs, strings.ReplaceAll(strings.TrimPrefix(path, dataDir)[1:], "\\", "/"))
	}

	json.NewEncoder(w).Encode(dbs)
}

func writeDbTables(w http.ResponseWriter, r *http.Request, dataDir string, dbname string) {
	m := mdbx.PathDbMap()
	db, ok := m[filepath.Join(dataDir, dbname)]
	if !ok {
		http.Error(w, fmt.Sprintf(`"%s" is not in the list of allowed dbs`, dbname), http.StatusNotFound)
		return
	}
	type table struct {
		Name  string `json:"name"`
		Count uint64 `json:"count"`
		Size  uint64 `json:"size"`
	}

	var tables []table

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var e error
		buckets, e := tx.ListTables()
		if e != nil {
			return e
		}

		for _, bucket := range buckets {
			size, e := tx.BucketSize(bucket)
			if e != nil {
				return e
			}

			var count uint64

			if e := db.View(context.Background(), func(tx kv.Tx) error {
				count, e = tx.Count(bucket)
				if e != nil {
					return e
				}

				return nil
			}); e != nil {
				return e
			}

			tables = append(tables, table{bucket, count, size})
		}

		return nil
	}); err != nil {
		http.Error(w, fmt.Sprintf(`failed to list tables in "%s": %v`, dbname, err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tables)
}

func writeDbRead(w http.ResponseWriter, r *http.Request, dataDir string, dbname string, table string, key []byte, offset int64, limit int64) {
	m := mdbx.PathDbMap()
	db, ok := m[filepath.Join(dataDir, dbname)]
	if !ok {
		fmt.Fprintf(w, "ERROR: path %s is not in the list of allowed paths", dbname)
		return
	}

	var results [][2][]byte
	var count uint64

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		c, e := tx.Cursor(table)
		if e != nil {
			return e
		}
		defer c.Close()

		count, e = tx.Count(table)

		if e != nil {
			return e
		}

		var k, v []byte
		if key == nil {
			if k, v, e = c.First(); e != nil {
				return e
			}
		} else if k, v, e = c.Seek(key); e != nil {
			return e
		}

		var pos int64

		for e == nil && k != nil && pos < offset {
			//TODO - not sure if this is a good idea it may be slooooow
			k, _, e = c.Next()
			pos++
		}

		for e == nil && k != nil && (limit == 0 || int64(len(results)) < limit) {
			results = append(results, [2][]byte{k, v})
			k, v, e = c.Next()
		}
		return nil
	}); err != nil {
		fmt.Fprintf(w, "ERROR: reading table %s in %s: %v\n", table, dbname, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{"))
	fmt.Fprintf(w, `"offset":%d`, offset)
	if limit > 0 {
		fmt.Fprintf(w, `,"limit":%d`, limit)
	}
	fmt.Fprintf(w, `,"count":%d`, count)
	if len(results) > 0 {
		var comma string
		w.Write([]byte(`,"results":{`))
		for _, result := range results {
			fmt.Fprintf(w, `%s"%s":"%s"`, comma, base64.URLEncoding.EncodeToString(result[0]), base64.URLEncoding.EncodeToString(result[1]))

			if comma == "" {
				comma = ","
			}
		}
		w.Write([]byte("}"))
	}
	w.Write([]byte("}"))
}
