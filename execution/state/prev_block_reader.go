package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// PrevBlockReader is a per-worker committed-base reader whose prev-block chain is
// swapped per task via SetBlock, so one long-lived worker reader can serve every
// block: it holds a stable raw base and, for the block a task belongs to, layers
// the finalized-but-not-yet-committed prior blocks (PrevBlockList.Below) in
// front of it. SetBlock is called once per task (window-sized rebuild, cheap);
// the read methods carry no per-read allocation. The IBS holds one stable
// PrevBlockReader, so switching blocks never rebuilds the IBS.
type PrevBlockReader struct {
	base  StateReader
	reg   *PrevBlockList
	chain StateReader
}

func NewPrevBlockReader(base StateReader, reg *PrevBlockList) *PrevBlockReader {
	return &PrevBlockReader{base: base, reg: reg, chain: base}
}

// PrevBlockBase wraps raw with the prev-block layers for a block known at
// construction — for the finalize / calcFees readers, which build a fresh reader
// per block rather than reusing one long-lived per-worker reader. Returns raw
// unchanged when there are no prior blocks to layer.
func PrevBlockBase(raw StateReader, list *PrevBlockList, blockNum uint64) StateReader {
	if list == nil {
		return raw
	}
	return layerVersionMaps(raw, list.Before(blockNum))
}

// SetBlock re-layers the prev-block for the block a task belongs to: the finalized
// maps of blocks < blockNum in front of the raw base. Committed (dropped) blocks
// are already gone from the registry, so the chain is at most window-deep.
func (o *PrevBlockReader) SetBlock(blockNum uint64) {
	o.chain = layerVersionMaps(o.base, o.reg.Before(blockNum))
}

func (o *PrevBlockReader) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	return o.chain.ReadAccountData(a)
}
func (o *PrevBlockReader) ReadAccountDataForDebug(a accounts.Address) (*accounts.Account, error) {
	return o.chain.ReadAccountDataForDebug(a)
}
func (o *PrevBlockReader) ReadAccountStorage(a accounts.Address, k accounts.StorageKey) (uint256.Int, bool, error) {
	return o.chain.ReadAccountStorage(a, k)
}
func (o *PrevBlockReader) HasStorage(a accounts.Address) (bool, error) {
	return o.chain.HasStorage(a)
}
func (o *PrevBlockReader) ReadAccountCode(a accounts.Address) ([]byte, error) {
	return o.chain.ReadAccountCode(a)
}
func (o *PrevBlockReader) ReadAccountCodeSize(a accounts.Address) (int, error) {
	return o.chain.ReadAccountCodeSize(a)
}
func (o *PrevBlockReader) ReadAccountIncarnation(a accounts.Address) (uint64, error) {
	return o.chain.ReadAccountIncarnation(a)
}
func (o *PrevBlockReader) SetTrace(v bool, prefix string) { o.chain.SetTrace(v, prefix) }
func (o *PrevBlockReader) Trace() bool                    { return o.chain.Trace() }
func (o *PrevBlockReader) TracePrefix() string            { return o.chain.TracePrefix() }
