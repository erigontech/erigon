package accounts

import (
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/erigontech/erigon/common"
)

// Measurement probe: WARM_INTERN=<n> interns n random addresses at start-up (a warmed process),
// INTERN_COUNT=1 counts first, repeated and re-created interns per newPayload, ProbeHold pins handles until the next one.
var (
	probeCount = os.Getenv("INTERN_COUNT") == "1"
	probeOn    atomic.Bool
	probeGen   atomic.Uint32
	probeSeen  sync.Map // common.Address -> probeEntry

	probeCalls, probeMake, probeNew, probeExisting, probeRecreatedBlock, probeRecreatedOld atomic.Int64

	probePre atomic.Int64

	probeHeldMu sync.Mutex
	probeHeld   []Address
)

type probeEntry struct {
	p   uintptr
	gen uint32
}

func init() {
	if n, _ := strconv.Atoi(os.Getenv("WARM_INTERN")); n > 0 {
		warmIntern(n)
	}
	probeOn.Store(true)
}

func warmIntern(n int) {
	t0 := time.Now()
	const workers = 8
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 42))
			var a common.Address
			for range n / workers {
				for i := 0; i < len(a); i += 8 {
					v := r.Uint64()
					for j := 0; j < 8 && i+j < len(a); j++ {
						a[i+j] = byte(v >> (8 * j))
					}
				}
				InternAddress(a)
			}
		})
	}
	wg.Wait()
	runtime.GC()
	runtime.GC()
	fmt.Fprintf(os.Stderr, "[intern-probe] warm n=%d elapsed=%s\n", n, time.Since(t0))
}

func probeCountCall() {
	if probeCount {
		probeCalls.Add(1)
	}
}

func probeCountMake(a common.Address, h Address) {
	if !probeCount || !probeOn.Load() {
		return
	}
	probeMake.Add(1)
	p := uintptr(unsafe.Pointer(*(**common.Address)(unsafe.Pointer(&h))))
	g := probeGen.Load()
	v, loaded := probeSeen.LoadOrStore(a, probeEntry{p, g})
	switch e := v.(probeEntry); {
	case !loaded:
		probeNew.Add(1)
	case e.p == p:
		probeExisting.Add(1)
	default:
		if e.gen == g {
			probeRecreatedBlock.Add(1)
		} else {
			probeRecreatedOld.Add(1)
		}
		probeSeen.Store(a, probeEntry{p, g})
	}
}

// ProbeHold keeps h alive until the next ProbeBeginBlock.
func ProbeHold(h Address) {
	probeHeldMu.Lock()
	probeHeld = append(probeHeld, h)
	probeHeldMu.Unlock()
}

func ProbeBeginBlock() {
	probeHeldMu.Lock()
	probeHeld = nil
	probeHeldMu.Unlock()
	probeGen.Add(1)
}

// ProbeEndBlock returns the counts of the block and resets them.
func ProbeEndBlock() string {
	if !probeCount && probePre.Load() == 0 {
		return ""
	}
	return fmt.Sprintf("npCalls=%d npMake=%d npNew=%d npExisting=%d npRecBlock=%d npRecOld=%d npPre=%s",
		probeCalls.Swap(0), probeMake.Swap(0), probeNew.Swap(0), probeExisting.Swap(0), probeRecreatedBlock.Swap(0), probeRecreatedOld.Swap(0), time.Duration(probePre.Swap(0)))
}

// ProbePreintern interns and holds addrs on 16 goroutines and records how long it took.
func ProbePreintern(addrs []common.Address) {
	t0 := time.Now()
	const workers = 16
	held := make([]Address, len(addrs))
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := w; i < len(addrs); i += workers {
				held[i] = InternAddress(addrs[i])
			}
		})
	}
	wg.Wait()
	probeHeldMu.Lock()
	probeHeld = append(probeHeld, held...)
	probeHeldMu.Unlock()
	probePre.Store(int64(time.Since(t0)))
}
