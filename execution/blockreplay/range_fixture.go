package blockreplay

import (
	"encoding/gob"
	"fmt"
	"os"
)

// RangeFixture is the exec-witness for a contiguous range of blocks streamed
// through one accumulating SharedDomains, for reproducing cross-block /
// intra-block parallel-execution hazards a single-block replay cannot exercise.
//
// Blocks holds one single-block Fixture per block, in ascending order. The
// merged pre-range witness keeps the earliest block's pre-value for each key:
// a key a later block reads was either written by an earlier in-range block
// (served from the accumulated domains) or unchanged since the range start.
// Outputs is the range-final post-state from canonical history at the last
// block's post-txNum.
type RangeFixture struct {
	Blocks  []*Fixture
	Outputs *Outputs
}

func (rf *RangeFixture) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewEncoder(f).Encode(rf)
}

func LoadRange(path string) (*RangeFixture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rf := &RangeFixture{}
	if err := gob.NewDecoder(f).Decode(rf); err != nil {
		return nil, err
	}
	if len(rf.Blocks) == 0 {
		return nil, fmt.Errorf("range fixture has no blocks")
	}
	return rf, nil
}

// SingleBlockRange wraps one Fixture as a one-block RangeFixture so a
// single-block fixture can drive the range replay.
func SingleBlockRange(fx *Fixture) *RangeFixture {
	return &RangeFixture{Blocks: []*Fixture{fx}, Outputs: fx.Outputs}
}

// MergedWitness collapses the per-block witnesses into a single Fixture whose
// pre-state maps are the keep-earliest union, for seeding a witness
// SharedDomains. Its BlockRLP/ParentHeaderRLP/Ancestors describe the first
// block; block streaming uses the per-block list, not this single BlockRLP.
func (rf *RangeFixture) MergedWitness() *Fixture {
	m := newFixture()
	m.BlockRLP = rf.Blocks[0].BlockRLP
	m.ParentHeaderRLP = rf.Blocks[0].ParentHeaderRLP
	m.Senders = rf.Blocks[0].Senders
	for _, b := range rf.Blocks {
		for a, d := range b.Accounts {
			if _, ok := m.Accounts[a]; !ok {
				m.Accounts[a] = d
			}
		}
		for a, code := range b.Code {
			if _, ok := m.Code[a]; !ok {
				m.Code[a] = code
			}
		}
		for a, slots := range b.Storage {
			inner := m.Storage[a]
			if inner == nil {
				inner = map[[32]byte][32]byte{}
				m.Storage[a] = inner
			}
			for k, v := range slots {
				if _, ok := inner[k]; !ok {
					inner[k] = v
				}
			}
		}
		for n, h := range b.Ancestors {
			if _, ok := m.Ancestors[n]; !ok {
				m.Ancestors[n] = h
			}
		}
	}
	m.Outputs = rf.Outputs
	return m
}
