package engineapi

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"time"

	"github.com/felixge/fgprof"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
)

var (
	npProfMode = dbg.EnvString("NP_PROF", "")
	npProfHash = dbg.EnvStrings("NP_PROF_HASH", "+", nil)
	npProfHz   = dbg.EnvInt("NP_PROF_HZ", 1000)
)

// npProfile profiles one newPayload whose block hash starts with an NP_PROF_HASH
// prefix into $TMPDIR/npprof; the returned func stops it.
func npProfile(hash common.Hash, t0 time.Time) func() {
	h := hash.Hex()
	if npProfMode == "" || !slices.ContainsFunc(npProfHash, func(p string) bool { return p != "" && strings.HasPrefix(h, p) }) {
		return func() {}
	}
	dir := filepath.Join(os.TempDir(), "npprof")
	_ = os.MkdirAll(dir, 0o755)
	f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s-%d.%s.pb.gz", h[:10], t0.UnixMicro(), npProfMode)))
	if err != nil {
		return func() {}
	}
	switch npProfMode {
	case "fgprof":
		stop := fgprof.Start(f, fgprof.FormatPprof)
		return func() { _ = stop(); _ = f.Close() }
	default:
		runtime.SetCPUProfileRate(npProfHz)
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return func() {}
		}
		return func() { pprof.StopCPUProfile(); _ = f.Close() }
	}
}
