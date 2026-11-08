package engineapi

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
)

// Probe knob WARM_PAGES_FILE=<file of "<path under its dir> <offset>">: each newPayload first reads those pages
// (64 at a time) and logs the time, so newPayload minus that time bounds prefetching them. Bound only.
var warmPagesFile = os.Getenv("WARM_PAGES_FILE")

func warmPages() {
	if warmPagesFile == "" {
		return
	}
	start := time.Now()
	f, err := os.Open(warmPagesFile)
	if err != nil {
		log.Warn("[probe] warm pages", "err", err)
		return
	}
	defer f.Close()
	root := filepath.Dir(warmPagesFile)
	files := map[string]*os.File{}
	type page struct {
		f   *os.File
		off int64
	}
	var pages []page
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		p := strings.Fields(sc.Text())
		if len(p) != 2 {
			continue
		}
		off, err := strconv.ParseInt(p[1], 10, 64)
		if err != nil {
			continue
		}
		fd, ok := files[p[0]]
		if !ok {
			if fd, err = os.Open(filepath.Join(root, p[0])); err != nil {
				continue
			}
			files[p[0]] = fd
		}
		pages = append(pages, page{fd, off})
	}
	var g errgroup.Group
	g.SetLimit(64)
	for _, p := range pages {
		g.Go(func() error {
			var buf [4096]byte
			_, _ = p.f.ReadAt(buf[:], p.off)
			return nil
		})
	}
	_ = g.Wait()
	for _, fd := range files {
		fd.Close()
	}
	log.Info("[probe] warm pages", "pages", len(pages), "files", len(files), "took", time.Since(start))
}
