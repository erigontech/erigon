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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon/turbo/logging"
)

func SetupLogsAccess(ctx *cli.Context, metricsMux *http.ServeMux) {
	if metricsMux == nil {
		return
	}

	dirPath := ctx.String(logging.LogDirPathFlag.Name)
	if dirPath == "" {
		datadir := ctx.String("datadir")
		if datadir != "" {
			dirPath = filepath.Join(datadir, "logs")
		}
	}
	if dirPath == "" {
		return
	}
	metricsMux.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		writeLogsList(w, dirPath)
	})
	metricsMux.HandleFunc("/logs/", func(w http.ResponseWriter, r *http.Request) {
		writeLogsRead(w, r, dirPath)
	})
}

func writeLogsList(w http.ResponseWriter, dirPath string) {
	entries, err := dir.ReadDir(dirPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list directory %s: %v", dirPath, err), http.StatusInternalServerError)
		return
	}

	infos := make([]fs.FileInfo, 0, len(entries))

	for _, entry := range entries {
		fileInfo, err := os.Stat(filepath.Join(dirPath, entry.Name()))
		if err != nil {
			http.Error(w, fmt.Sprintf("Can't stat file %s: %v", entry.Name(), err), http.StatusInternalServerError)
			return
		}
		if fileInfo.IsDir() {
			continue
		}
		infos = append(infos, fileInfo)
	}

	type file struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
	}

	files := make([]file, 0, len(infos))

	for _, fileInfo := range infos {
		files = append(files, file{Name: fileInfo.Name(), Size: fileInfo.Size()})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func writeLogsRead(w http.ResponseWriter, r *http.Request, dirPath string) {
	file := path.Base(r.URL.Path)

	if file == "/" || file == "." {
		http.Error(w, "file is required - specify the name of log file to read", http.StatusBadRequest)
		return
	}

	offset, err := offsetValue(r.URL.Query())

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fileInfo, err := os.Stat(filepath.Join(dirPath, file))

	if err != nil {
		http.Error(w, fmt.Sprintf("Can't stat file %s: %v", file, err), http.StatusInternalServerError)
		return
	}

	if fileInfo.IsDir() {
		http.Error(w, file+" is a directory, needs to be a file", http.StatusInternalServerError)
		return
	}

	if offset > fileInfo.Size() {
		http.Error(w, fmt.Sprintf("offset %d must not be greater than this file size %d", offset, fileInfo.Size()), http.StatusBadRequest)
		return
	}

	f, err := os.Open(filepath.Join(dirPath, file))

	if err != nil {
		http.Error(w, fmt.Sprintf("Can't opening file %s: %v\n", file, err), http.StatusInternalServerError)
		return
	}

	limit, err := limitValue(r.URL.Query(), fileInfo.Size())

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	buf := make([]byte, limit)

	if _, err := f.Seek(offset, 0); err != nil {
		http.Error(w, fmt.Sprintf("seek failed for file: %s to %d: %v", file, offset, err), http.StatusInternalServerError)
		return
	}

	var n int
	var readTotal int

	for n, err = f.Read(buf[readTotal:]); err == nil && readTotal < len(buf); n, err = f.Read(buf[readTotal:]) {
		readTotal += n
	}

	if err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, fmt.Sprintf("Reading failed for: %s at %d: %v\n", file, readTotal, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(readTotal), 10))
	w.Header().Set("X-Offset", strconv.FormatInt(offset, 10))
	w.Header().Set("X-Limit", strconv.FormatInt(limit, 10))
	w.Header().Set("X-Size", strconv.FormatInt(fileInfo.Size(), 10))
	w.Write(buf[:readTotal])
}

func limitValue(values url.Values, def int64) (int64, error) {
	limitStr := values.Get("limit")

	var limit int64
	var err error

	if limitStr == "" {
		limit = def
	} else {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
	}

	if err != nil {
		return 0, fmt.Errorf("limit %s is not a int64 number: %v", limitStr, err)
	}

	return limit, nil
}

func offsetValue(values url.Values) (int64, error) {

	offsetStr := values.Get("offset")

	var offset int64
	var err error

	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)

		if err != nil {
			return 0, fmt.Errorf("offset %s is not a int64 number: %v", offsetStr, err)
		}
	}

	if offset < 0 {
		return 0, fmt.Errorf("offset %d must be non-negative", offset)
	}

	return offset, nil
}
