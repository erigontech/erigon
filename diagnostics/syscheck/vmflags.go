// Copyright 2026 The Erigon Authors
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

package syscheck

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
)

// Mapping is one file-backed VMA of this process and its readahead hint.
type Mapping struct {
	Path       string
	Start      uint64
	End        uint64
	Random     bool // VM_RAND_READ — MADV_RANDOM
	Sequential bool // VM_SEQ_READ  — MADV_SEQUENTIAL
}

func (m Mapping) Advice() string {
	switch {
	case m.Random:
		return "random"
	case m.Sequential:
		return "sequential"
	default:
		return "normal"
	}
}

func (m Mapping) SizeBytes() uint64 { return m.End - m.Start }

// FileMappings lists this process's file-backed mappings. Linux only — madvise
// state lives in VmFlags, which no other platform exposes; elsewhere it returns
// nil. Reading smaps walks page tables per VMA, so call it on demand, not in a loop.
func FileMappings() ([]Mapping, error) {
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	f, err := os.Open("/proc/self/smaps")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseSmaps(f)
}

// LogNonRandomFileMappings logs every file-backed mapping under pathPrefix that is
// not MADV_RANDOM. An empty prefix reports all of them.
func LogNonRandomFileMappings(logger log.Logger, pathPrefix string) {
	all, err := FileMappings()
	if err != nil {
		logger.Debug("[mmap] cannot read /proc/self/smaps", "err", err)
		return
	}
	if all == nil {
		return
	}
	bad := nonRandom(all, pathPrefix)
	if len(bad) == 0 {
		logger.Info("[mmap] all file mappings are MADV_RANDOM", "checked", len(all), "prefix", pathPrefix)
		return
	}
	for _, m := range bad {
		logger.Warn("[mmap] not MADV_RANDOM", "file", m.Path, "advice", m.Advice(), "mb", m.SizeBytes()/(1<<20))
	}
	logger.Warn("[mmap] non-random file mappings", "count", len(bad), "of", len(all), "prefix", pathPrefix)
}

// ServeFileMappings answers /debug/mmap with this process's file mappings that are
// not MADV_RANDOM. Optional query param `prefix` restricts to one path prefix;
// `all=true` returns every file mapping regardless of advice.
func ServeFileMappings(w http.ResponseWriter, r *http.Request) {
	mappings, err := FileMappings()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := struct {
		Supported bool      `json:"supported"`
		Total     int       `json:"total"`
		NonRandom []Mapping `json:"nonRandom"`
	}{
		Supported: runtime.GOOS == "linux",
		Total:     len(mappings),
	}
	prefix := r.URL.Query().Get("prefix")
	if r.URL.Query().Get("all") == "true" {
		resp.NonRandom = mappings
	} else {
		resp.NonRandom = nonRandom(mappings, prefix)
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func nonRandom(all []Mapping, pathPrefix string) []Mapping {
	var out []Mapping
	for _, m := range all {
		if m.Random {
			continue
		}
		if pathPrefix != "" && !strings.HasPrefix(m.Path, pathPrefix) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// parseSmaps reads /proc/<pid>/smaps: a header line per VMA followed by indented
// fields, of which only VmFlags matters here. Mappings without a real file path
// (anonymous, [heap], [stack], ...) are dropped.
func parseSmaps(r io.Reader) ([]Mapping, error) {
	var out []Mapping
	cur := -1
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "VmFlags:") {
			if cur >= 0 {
				for _, fl := range strings.Fields(line[len("VmFlags:"):]) {
					switch fl {
					case "rr":
						out[cur].Random = true
					case "sr":
						out[cur].Sequential = true
					}
				}
			}
			continue
		}
		start, end, path, ok := parseSmapsHeader(line)
		if !ok {
			continue
		}
		cur = -1
		if path == "" || strings.HasPrefix(path, "[") {
			continue
		}
		out = append(out, Mapping{Path: path, Start: start, End: end})
		cur = len(out) - 1
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// parseSmapsHeader decodes `start-end perms offset dev inode [path]`. The path may
// contain spaces, so it is taken as the remainder after the inode field.
func parseSmapsHeader(line string) (start, end uint64, path string, ok bool) {
	dash := strings.IndexByte(line, '-')
	if dash <= 0 {
		return 0, 0, "", false
	}
	start, err := strconv.ParseUint(line[:dash], 16, 64)
	if err != nil {
		return 0, 0, "", false
	}
	rest := line[dash+1:]
	sp := strings.IndexByte(rest, ' ')
	if sp <= 0 {
		return 0, 0, "", false
	}
	end, err = strconv.ParseUint(rest[:sp], 16, 64)
	if err != nil {
		return 0, 0, "", false
	}
	// perms, offset, dev, inode — then everything left is the path
	rest = rest[sp+1:]
	for range 4 {
		rest = strings.TrimLeft(rest, " ")
		sp = strings.IndexByte(rest, ' ')
		if sp < 0 {
			return start, end, "", true
		}
		rest = rest[sp+1:]
	}
	return start, end, strings.TrimLeft(rest, " "), true
}
