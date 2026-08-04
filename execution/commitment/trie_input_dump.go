package commitment

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

// Diagnostic (DUMP_TI): dump the per-block commitment trie inputs (touched keys +
// their final values fed to the trie) for a block range, so a parallel run (which
// may compute a wrong root) can be diffed against a serial run to localize the
// wrong previous-state read. Off unless DUMP_TI_FROM is set.
var (
	dumpTIFrom, dumpTITo = func() (uint64, uint64) {
		from, _ := strconv.ParseUint(os.Getenv("DUMP_TI_FROM"), 10, 64)
		to, _ := strconv.ParseUint(os.Getenv("DUMP_TI_TO"), 10, 64)
		if to == 0 {
			to = from
		}
		return from, to
	}()
	dumpTILabel = os.Getenv("DUMP_TI_LABEL")
	dumpTIMu    sync.Mutex

	// TrieInputDumpBlock is set by the commitment caller (per block) so the serial
	// HashSort path knows which block's inputs it is emitting.
	TrieInputDumpBlock atomic.Uint64
)

// TrieInputDumpActive reports whether inputs for blockNum should be dumped.
func TrieInputDumpActive(blockNum uint64) bool {
	return dumpTIFrom != 0 && blockNum >= dumpTIFrom && blockNum <= dumpTITo
}

// DumpTrieInputLine appends one line to /tmp/ti-<label>-<block>.txt.
func DumpTrieInputLine(blockNum uint64, line string) {
	dumpTIMu.Lock()
	defer dumpTIMu.Unlock()
	f, err := os.OpenFile(fmt.Sprintf("/tmp/ti-%s-%d.txt", dumpTILabel, blockNum), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, line)
}
