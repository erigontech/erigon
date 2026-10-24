package commitment

import (
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

type Metrics struct {
	addressKeys      atomic.Uint64
	storageKeys      atomic.Uint64
	loadBranch       atomic.Uint64
	loadAccount      atomic.Uint64
	loadStorage      atomic.Uint64
	updateBranch     atomic.Uint64
	unfolds          atomic.Uint64
	folds            atomic.Uint64
	roundKeys        atomic.Uint64
	branchReadBytes  atomic.Uint64
	branchWriteBytes atomic.Uint64
}

type MetricValues struct {
	AddressKeys  uint64
	StorageKeys  uint64
	LoadBranch   uint64
	LoadAccount  uint64
	LoadStorage  uint64
	UpdateBranch uint64
	Unfolds      uint64
	Folds        uint64
	// RoundKeys is the distinct key count of one round — Process resets the
	// counters on both engines, so nothing here spans rounds. AddressKeys and
	// StorageKeys count cell traversals instead, which the parallel engine
	// inflates by re-walking subtrees on mount+replay.
	RoundKeys        uint64
	BranchReadBytes  uint64
	BranchWriteBytes uint64
}

func (m *Metrics) AsValues() MetricValues {
	return MetricValues{
		AddressKeys:      m.addressKeys.Load(),
		StorageKeys:      m.storageKeys.Load(),
		LoadBranch:       m.loadBranch.Load(),
		LoadAccount:      m.loadAccount.Load(),
		LoadStorage:      m.loadStorage.Load(),
		UpdateBranch:     m.updateBranch.Load(),
		Unfolds:          m.unfolds.Load(),
		Folds:            m.folds.Load(),
		RoundKeys:        m.roundKeys.Load(),
		BranchReadBytes:  m.branchReadBytes.Load(),
		BranchWriteBytes: m.branchWriteBytes.Load(),
	}
}

func (m *Metrics) logMetrics() []any {
	return []any{
		"akeys", common.PrettyCounter(m.addressKeys.Load()), "skeys", common.PrettyCounter(m.storageKeys.Load()),
		"rdb", common.PrettyCounter(m.loadBranch.Load()), "rda", common.PrettyCounter(m.loadAccount.Load()),
		"rds", common.PrettyCounter(m.loadStorage.Load()), "wrb", common.PrettyCounter(m.updateBranch.Load()),
		"fld", common.PrettyCounter(m.folds.Load()), "ufld", common.PrettyCounter(m.unfolds.Load()),
	}
}

func (m *Metrics) Reset() {
	m.addressKeys.Store(0)
	m.storageKeys.Store(0)
	m.loadBranch.Store(0)
	m.loadAccount.Store(0)
	m.loadStorage.Store(0)
	m.updateBranch.Store(0)
	m.unfolds.Store(0)
	m.folds.Store(0)
	m.roundKeys.Store(0)
	m.branchReadBytes.Store(0)
	m.branchWriteBytes.Store(0)
}

func (m *Metrics) Updates(plainKey []byte) {
	if len(plainKey) == length.Addr {
		m.addressKeys.Add(1)
	} else {
		m.storageKeys.Add(1)
	}
}

// AddBranchRead records one branch read of n bytes.
func (m *Metrics) AddBranchRead(n int) { m.branchReadBytes.Add(uint64(n)) }

// AddBranchWrite records one branch write of n bytes.
func (m *Metrics) AddBranchWrite(n int) { m.branchWriteBytes.Add(uint64(n)) }

// AddRoundKeys records the distinct-key count of one finished round. Not
// merged between tries: the engine that ran the round owns this number, while
// each worker only sees its own subtree.
func (m *Metrics) AddRoundKeys(n uint64) { m.roundKeys.Add(n) }

// Merge folds src's counters into m. The parallel trie gives every mount
// worker its own Metrics — an atomic add on a shared line in the fold loop
// would cost more than the counter is worth — and merges once per round.
func (m *Metrics) Merge(src *Metrics) {
	if src == nil || m == src {
		return
	}
	m.addressKeys.Add(src.addressKeys.Load())
	m.storageKeys.Add(src.storageKeys.Load())
	m.loadBranch.Add(src.loadBranch.Load())
	m.loadAccount.Add(src.loadAccount.Load())
	m.loadStorage.Add(src.loadStorage.Load())
	m.updateBranch.Add(src.updateBranch.Load())
	m.unfolds.Add(src.unfolds.Load())
	m.folds.Add(src.folds.Load())
	m.branchReadBytes.Add(src.branchReadBytes.Load())
	m.branchWriteBytes.Add(src.branchWriteBytes.Load())
}
