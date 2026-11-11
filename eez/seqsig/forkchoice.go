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

package seqsig

import (
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

const (
	// MaxAncestryWalk bounds every ancestry walk: the blocks fetched back to
	// a local block, the headers read down to safe, and how far a target may
	// lead the local head.
	MaxAncestryWalk = 1024
	// FetchBatch is the most blocks one Fetch asks for.
	FetchBatch = 64
)

var (
	ErrConflictsSafe   = errors.New("conflicts with safe")
	ErrHeld            = errors.New("safe lags the L1 batch index")
	ErrAhead           = errors.New("target too far ahead of the local head")
	ErrUnverifiable    = errors.New("no local ancestor within the walk bound")
	ErrUnknownAncestry = errors.New("ancestry down to safe not known")
	ErrNotServed       = errors.New("block not served")
	ErrBadAncestor     = errors.New("served block is not the one requested")
	ErrNoPeers         = errors.New("no connected devp2p peers")
)

// BlockID is a block's number and hash.
type BlockID struct {
	Number uint64
	Hash   common.Hash
}

// Chain is the node's view of its own L2 chain.
type Chain interface {
	// Head returns the canonical head.
	Head() (BlockID, error)
	// Safe returns the safe block; ok is false until derivation sets one.
	Safe() (safe BlockID, ok bool, err error)
	// Header returns the number and parent of a block the node has,
	// canonical or not.
	Header(hash common.Hash) (number uint64, parent common.Hash, ok bool, err error)
	// CanonicalHash returns the hash of the canonical block at number.
	CanonicalHash(number uint64) (hash common.Hash, ok bool, err error)
}

// Inputs are the facts a decision reads besides the local chain.
type Inputs struct {
	// Candidates are the tracked heads, highest version first.
	Candidates []*VerifiedHead
	// Indexed is the highest L2 block derivation has indexed from L1
	// batches; safe follows it once the batch is reconciled.
	Indexed uint64
	// Peers is the number of connected devp2p peers.
	Peers int
	// Fetched holds the answer to each earlier Fetch, newest block first,
	// keyed by its From.
	Fetched map[common.Hash][]*types.Block
}

// Action is what a decision asks of the follower.
type Action uint8

const (
	// Ignore leaves the head where it is.
	Ignore Action = iota
	// Fetch asks devp2p peers for Count blocks back from From; the answer
	// goes into Inputs.Fetched for the next decision.
	Fetch
	// Adopt inserts Blocks, then moves the head to the target.
	Adopt
	// Rewind moves the head to the target, a block the node has.
	Rewind
)

func (a Action) String() string {
	switch a {
	case Ignore:
		return "ignore"
	case Fetch:
		return "fetch"
	case Adopt:
		return "adopt"
	case Rewind:
		return "rewind"
	}
	return "unknown"
}

// Decision is the next step toward the unsafe head.
type Decision struct {
	Action Action
	// Target is the candidate the decision is about, nil if none is left.
	Target *VerifiedHead
	// From and Count are a Fetch's request.
	From  common.Hash
	Count uint64
	// Blocks are an Adopt's missing blocks, oldest first, the target last.
	Blocks []*types.Block
	// Err is why an Ignore leaves the head, nil if no candidate needs a move.
	Err error
	// Skipped are the higher candidates passed over for conflicting with
	// safe.
	Skipped []Skip
}

// Skip is a candidate passed over and why.
type Skip struct {
	Head *VerifiedHead
	Err  error
}

// Decide returns the next step toward the highest-version candidate that
// descends from safe. A candidate that provably does not is skipped; one
// whose ancestry is unknown stops the decision, so a lower version never
// wins on a missing block. Nothing moves while derivation has indexed a batch
// above safe, since the heights in between are settled on L1.
//
// A Fetch is answered through Inputs.Fetched and decided again. A caller
// whose derivation runs concurrently decides again under the lock that
// excludes it before acting on Adopt or Rewind.
func Decide(c Chain, in Inputs) (Decision, error) {
	safe, hasSafe, err := c.Safe()
	if err != nil {
		return Decision{}, err
	}
	var anchor *BlockID
	if hasSafe {
		anchor = &safe
	}
	var skipped []Skip
	for _, cand := range in.Candidates {
		h := cand.Head
		// The head alone can prove a conflict, so a source whose ancestors
		// nobody serves stops blocking lower candidates once safe passes it.
		v, err := extendsSafe(c, anchor, BlockID{h.Number, h.BlockHash}, h.ParentHash)
		if err != nil {
			return Decision{}, err
		}
		if v == conflicts {
			skipped = append(skipped, Skip{Head: cand, Err: ErrConflictsSafe})
			continue
		}
		if in.Indexed > safe.Number {
			return Decision{Target: cand, Err: fmt.Errorf("%w: indexed %d, safe %d", ErrHeld, in.Indexed, safe.Number), Skipped: skipped}, nil
		}
		d, err := walk(c, anchor, in, cand)
		if err != nil {
			return Decision{}, err
		}
		if errors.Is(d.Err, ErrConflictsSafe) {
			skipped = append(skipped, Skip{Head: cand, Err: d.Err})
			continue
		}
		d.Target, d.Skipped = cand, skipped
		return d, nil
	}
	return Decision{Skipped: skipped}, nil
}

// walk follows cand back through parent hashes to the first block the node
// has, taking blocks from the envelope and from earlier fetches, and decides
// on that fork point.
func walk(c Chain, safe *BlockID, in Inputs, cand *VerifiedHead) (Decision, error) {
	head, err := c.Head()
	if err != nil {
		return Decision{}, err
	}
	// The fork point is at or below the local head, so no walk to a target
	// further ahead fits the bound.
	if cand.Head.Number > head.Number && cand.Head.Number-head.Number > MaxAncestryWalk {
		return Decision{Err: fmt.Errorf("%w: target %d, local head %d", ErrAhead, cand.Head.Number, head.Number)}, nil
	}
	var batch, blocks []*types.Block
	if cand.Block != nil {
		batch = []*types.Block{cand.Block}
	}
	number, hash := cand.Head.Number, cand.Head.BlockHash
	var fork BlockID
	var forkParent common.Hash
	for {
		n, parent, ok, err := c.Header(hash)
		if err != nil {
			return Decision{}, err
		}
		if ok {
			fork, forkParent = BlockID{n, hash}, parent
			break
		}
		if len(blocks) >= MaxAncestryWalk {
			return Decision{Err: fmt.Errorf("%w: %d blocks", ErrUnverifiable, len(blocks))}, nil
		}
		if len(batch) == 0 {
			served, asked := in.Fetched[hash]
			switch {
			case !asked && in.Peers == 0:
				return Decision{Err: ErrNoPeers}, nil
			case !asked:
				// Sized to the gap down to the local head, so one missed head
				// costs one block.
				count := min(max(number-min(number, head.Number), 1), FetchBatch)
				return Decision{Action: Fetch, From: hash, Count: count}, nil
			case len(served) == 0:
				return Decision{Err: fmt.Errorf("%w: %x", ErrNotServed, hash)}, nil
			}
			batch = served
		}
		b := batch[0]
		batch = batch[1:]
		if b.Hash() != hash {
			return Decision{Err: fmt.Errorf("%w: requested %x, got %x", ErrBadAncestor, hash, b.Hash())}, nil
		}
		number, hash = max(b.NumberU64(), 1)-1, b.ParentHash()
		blocks = append(blocks, b)
	}
	switch v, err := extendsSafe(c, safe, fork, forkParent); {
	case err != nil:
		return Decision{}, err
	case v == conflicts:
		return Decision{Err: fmt.Errorf("%w: fork point %d (%x)", ErrConflictsSafe, fork.Number, fork.Hash)}, nil
	case v == unverifiable:
		return Decision{Err: fmt.Errorf("%w: fork point %d (%x)", ErrUnknownAncestry, fork.Number, fork.Hash)}, nil
	}
	if len(blocks) == 0 {
		if fork.Hash == head.Hash {
			return Decision{}, nil
		}
		return Decision{Action: Rewind}, nil
	}
	slices.Reverse(blocks)
	return Decision{Action: Adopt, Blocks: blocks}, nil
}

type compat uint8

const (
	extends compat = iota
	conflicts
	unverifiable
)

// extendsSafe reports whether block id with the given parent descends from
// safe. A canonical block does by the fork-choice invariant; any other is
// walked through the headers the node has, at most MaxAncestryWalk of them.
func extendsSafe(c Chain, safe *BlockID, id BlockID, parent common.Hash) (compat, error) {
	switch {
	case safe == nil:
		return extends, nil
	case id.Number == safe.Number && id.Hash == safe.Hash:
		return extends, nil
	case id.Number <= safe.Number:
		return conflicts, nil
	}
	if h, ok, err := c.CanonicalHash(id.Number); err != nil {
		return 0, err
	} else if ok && h == id.Hash {
		return extends, nil
	}
	cursor, number := parent, id.Number-1
	for reads := 0; number > safe.Number; reads++ {
		if reads >= MaxAncestryWalk {
			return unverifiable, nil
		}
		_, p, ok, err := c.Header(cursor)
		if err != nil {
			return 0, err
		}
		if !ok {
			return unverifiable, nil
		}
		cursor, number = p, number-1
	}
	if cursor == safe.Hash {
		return extends, nil
	}
	return conflicts, nil
}
