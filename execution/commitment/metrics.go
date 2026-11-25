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
	LoadAccount      atomic.Uint64
	LoadStorage      atomic.Uint64
	updateBranch     atomic.Uint64
	Unfolds          atomic.Uint64
	Folds            atomic.Uint64
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
		LoadAccount:      m.LoadAccount.Load(),
		LoadStorage:      m.LoadStorage.Load(),
		UpdateBranch:     m.updateBranch.Load(),
		Unfolds:          m.Unfolds.Load(),
		Folds:            m.Folds.Load(),
		RoundKeys:        m.roundKeys.Load(),
		BranchReadBytes:  m.branchReadBytes.Load(),
		BranchWriteBytes: m.branchWriteBytes.Load(),
	}
}

func (m *Metrics) LogMetrics() []any {
	return []any{
		"akeys", common.PrettyCounter(m.addressKeys.Load()), "skeys", common.PrettyCounter(m.storageKeys.Load()),
		"rdb", common.PrettyCounter(m.loadBranch.Load()), "rda", common.PrettyCounter(m.LoadAccount.Load()),
		"rds", common.PrettyCounter(m.LoadStorage.Load()), "wrb", common.PrettyCounter(m.updateBranch.Load()),
		"fld", common.PrettyCounter(m.Folds.Load()), "ufld", common.PrettyCounter(m.Unfolds.Load()),
	}
}

func (m *Metrics) Reset() {
	m.addressKeys.Store(0)
	m.storageKeys.Store(0)
	m.loadBranch.Store(0)
	m.LoadAccount.Store(0)
	m.LoadStorage.Store(0)
	m.updateBranch.Store(0)
	m.Unfolds.Store(0)
	m.Folds.Store(0)
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

func (m *Metrics) AddBranchLoad(n uint64) { m.loadBranch.Add(n) }

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
	m.LoadAccount.Add(src.LoadAccount.Load())
	m.LoadStorage.Add(src.LoadStorage.Load())
	m.updateBranch.Add(src.updateBranch.Load())
	m.Unfolds.Add(src.Unfolds.Load())
	m.Folds.Add(src.Folds.Load())
	m.branchReadBytes.Add(src.branchReadBytes.Load())
	m.branchWriteBytes.Add(src.branchWriteBytes.Load())
}
