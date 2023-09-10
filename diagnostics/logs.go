package diagnostics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/turbo/logging"
)

func SetupLogsAccess(ctx *cli.Context, metricsMux *http.ServeMux) {
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
	metricsMux.HandleFunc("/debug/logs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeLogsList(w, dirPath)
	})
	metricsMux.HandleFunc("/debug/logs/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeLogsRead(w, r, dirPath)
	})
}

func writeLogsList(w http.ResponseWriter, dirPath string) {
	entries, err := os.ReadDir(dirPath)
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

	var files []file
	for _, fileInfo := range infos {
		files = append(files, file{Name: fileInfo.Name(), Size: fileInfo.Size()})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func writeLogsRead(w http.ResponseWriter, r *http.Request, dirPath string) {
	file := path.Base(r.URL.Path)

	if file == "/" || file == "." {
		http.Error(w, fmt.Sprintf("file is required - specify the name of log file to read"), http.StatusBadRequest)
		return
	}

	offsetStr := r.URL.Query().Get("offset")

	var offset int64
	var err error

	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)

		if err != nil {
			http.Error(w, fmt.Sprintf("offset %s is not a Uint64 number: %v", offsetStr, err), http.StatusBadRequest)
			return
		}
	}

	if offset < 0 {
		http.Error(w, fmt.Sprintf("offset %d must be non-negative", offset), http.StatusBadRequest)
		return
	}

	fileInfo, err := os.Stat(filepath.Join(dirPath, file))

	if err != nil {
		http.Error(w, fmt.Sprintf("Can't stat file %s: %v", file, err), http.StatusInternalServerError)
		return
	}

	if fileInfo.IsDir() {
		http.Error(w, fmt.Sprintf("%s is a directory, needs to be a file", file), http.StatusInternalServerError)
		return
	}

	if offset > fileInfo.Size() {
		http.Error(w, fmt.Sprintf("offset %d must not be greater than this file size %d", offset, fileInfo.Size()), http.StatusBadRequest)
		return
	}

	f, err := os.Open(filepath.Join(dirPath, file))

	if err != nil {
		fmt.Fprintf(w, "ERROR: opening file %s: %v\n", file, err)
		return
	}

	sizeStr := r.URL.Query().Get("size")

	var size int64

	if sizeStr == "" {
		size = fileInfo.Size()
	}

	if size == 0 {
		size, err = strconv.ParseInt(sizeStr, 10, 64)
	}

	if err != nil {
		http.Error(w, fmt.Sprintf("size %s is not a Uint64 number: %v", sizeStr, err), http.StatusBadRequest)
		return
	}

	buf := make([]byte, size)

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
	w.Header().Set("X-Offset", strconv.FormatInt(int64(offset), 10))
	w.Header().Set("X-Size", strconv.FormatInt(int64(fileInfo.Size()), 10))
	w.Write(buf[:readTotal])
}
