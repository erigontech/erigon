package diagnostics

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
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
	metricsMux.HandleFunc("/debug/metrics/logs/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeLogsList(w, dirPath)
	})
	metricsMux.HandleFunc("/debug/metrics/logs/read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeLogsRead(w, r, dirPath)
	})
}

func writeLogsList(w io.Writer, dirPath string) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Fprintf(w, "ERROR: listing directory %s: %v\n", dirPath, err)
		return
	}
	//nolint: prealloc
	var infos []fs.FileInfo
	for _, entry := range entries {
		fileInfo, err := os.Stat(filepath.Join(dirPath, entry.Name()))
		if err != nil {
			fmt.Fprintf(w, "ERROR: stat file %s: %v\n", entry.Name(), err)
			return
		}
		if fileInfo.IsDir() {
			continue
		}
		infos = append(infos, fileInfo)
	}
	fmt.Fprintf(w, "SUCCESS\n")
	for _, fileInfo := range infos {
		fmt.Fprintf(w, "%s | %d\n", fileInfo.Name(), fileInfo.Size())
	}
}

func writeLogsRead(w io.Writer, r *http.Request, dirPath string) {
	if err := r.ParseForm(); err != nil {
		fmt.Fprintf(w, "ERROR: parsing arguments: %v\n", err)
		return
	}
	file := r.Form.Get("file")
	if file == "" {
		fmt.Fprintf(w, "ERROR: file argument is required - specify the name of log file to read")
		return
	}
	fileInfo, err := os.Stat(filepath.Join(dirPath, file))
	if err != nil {
		fmt.Fprintf(w, "ERROR: stat file %s: %v\n", file, err)
		return
	}
	if fileInfo.IsDir() {
		fmt.Fprintf(w, "ERROR: %s is a directory, needs to be a file", file)
		return
	}
	offsetStr := r.Form.Get("offset")
	if offsetStr == "" {
		fmt.Fprintf(w, "ERROR: offset argument is required - specify where to start reading in the file")
		return
	}
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		fmt.Fprintf(w, "ERROR: offset %s is not a Uint64 number: %v\n", offsetStr, err)
		return
	}
	if offset < 0 {
		fmt.Fprintf(w, "ERROR: offset %d must be non-negative\n", offset)
		return
	}
	if offset > fileInfo.Size() {
		fmt.Fprintf(w, "ERROR: offset %d must be no greater than file size %d\n", offset, fileInfo.Size())
		return
	}
	f, err := os.Open(filepath.Join(dirPath, file))
	if err != nil {
		fmt.Fprintf(w, "ERROR: opening file %s: %v\n", file, err)
		return
	}
	var buf [16 * 1024]byte
	if _, err := f.Seek(offset, 0); err != nil {
		fmt.Fprintf(w, "ERROR: seeking in file: %s to %d: %v\n", file, offset, err)
		return
	}
	var n int
	var readTotal int
	for n, err = f.Read(buf[readTotal:]); err == nil && readTotal < len(buf); n, err = f.Read(buf[readTotal:]) {
		readTotal += n
	}
	if err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(w, "ERROR: reading in file: %s at %d: %v\n", file, readTotal, err)
		return
	}
	fmt.Fprintf(w, "SUCCESS: %d-%d/%d\n", offset, offset+int64(readTotal), fileInfo.Size())
	w.Write(buf[:readTotal])
}
