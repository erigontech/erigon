package domino

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
)

// Case defines an interface for storage of dominoes. it should be thread safe
type Case interface {
	// Checkpoint should get the state at or before the slot selected
	Checkpoint(ctx context.Context, slot uint64) (abstract.BeaconState, error)
	// Dominos should get a block
	Domino(ctx context.Context, slot uint64) (*cltypes.SignedBeaconBlock, error)
}

// Exhibit is for managing multiple domino runs
type Exhibit struct {
	c Case
	m machine.Interface
}

func NewExhibit(c Case, m machine.Interface) *Exhibit {
	return &Exhibit{
		c: c,
		m: m,
	}
}

// GetRun gets a run.
// in the future, the exhibit can be smarter and possibly reuse runs
// caller should call topple when they need to.
func (e *Exhibit) GetRun(ctx context.Context, slot uint64) (*Run, error) {
	r := NewRun(e.c, e.m)
	err := r.Reset(ctx, slot)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// In the future, we can potentially reuse runs. for now we don't
func (e *Exhibit) PutRun(*Run) {
}

func NewRun(c Case, m machine.Interface) *Run {
	return &Run{
		c: c,
		m: m,
	}
}

// A domino run is not thread safe and should be protected by a external mutex if needed to be called thread safe
type Run struct {
	c Case
	m machine.Interface

	s abstract.BeaconState
}

// Reset will reset the domino stack to the nearest checkpoint
func (r *Run) Reset(ctx context.Context, slot uint64) (err error) {
	// if we are not resetting for initialization, we check if we actually need to reset
	if r.s != nil {
		if r.s.Slot() == slot {
			return nil
		}
	}
	r.s, err = r.c.Checkpoint(ctx, slot)
	if err != nil {
		return err
	}
	return nil
}

func (r *Run) State() abstract.BeaconState {
	return r.s
}

func (r *Run) Current() uint64 {
	if r.s == nil {
		return 0
	}
	return r.s.Slot()
}

func (r *Run) ToppleTo(ctx context.Context, slot uint64) error {
	if r.s == nil {
		return fmt.Errorf("No state set")
	}
	if r.s.Slot() == slot {
		return nil
	}
	if r.s.Slot() > slot {
		return fmt.Errorf("Cannot restack dominos (%d -> %d)", r.s.Slot(), slot)
	}
	res := make(chan *cltypes.SignedBeaconBlock, 1)
	errCh := make(chan error)
	go func() {
		for i := r.s.Slot(); i <= slot; i++ {
			blk, err := r.c.Domino(ctx, i)
			if err != nil {
				errCh <- err
				return
			}
			res <- blk
		}
		close(res)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case val, ok := <-res:
			if !ok {
				return nil
			}
			err := machine.TransitionState(r.m, r.s, val)
			if err != nil {
				return err
			}
		case err := <-errCh:
			return err
		}
	}
}
