package stagedsync

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func TestHandleHeadersMsg(t *testing.T) {
	hd := NewHeaderDownload("", 10, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	})
	peer := PeerHandle(1)

	// Empty message
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 0 {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Single header
	var h types.Header
	h.Number = big.NewInt(5)
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Same header repeated twice
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h, &h}, peer); err == nil {
		if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != DuplicateHeaderPenalty {
			t.Errorf("expected DuplicateHeader penalty, got %s", peerPenalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Single header with a bad hash
	hd.badHeaders[h.Hash()] = struct{}{}
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h}, peer); err == nil {
		if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != BadBlockPenalty {
			t.Errorf("expected BadBlock penalty, got %s", peerPenalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two connected headers
	var h1, h2 types.Header
	h1.Number = big.NewInt(1)
	h1.Difficulty = big.NewInt(10)
	h2.Number = big.NewInt(2)
	h2.Difficulty = big.NewInt(1010)
	h2.ParentHash = h1.Hash()
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h1, &h2}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].headers) != 2 {
			t.Errorf("expected chainSegment of the length 2, got %d", len(chainSegments[0].headers))
		}
		if chainSegments[0].headers[0] != &h1 {
			t.Errorf("expected h1 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two connected headers with wrong numbers
	h2.Number = big.NewInt(3) // Child number 3, parent number 1
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h1, &h2}, peer); err == nil {
		if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != WrongChildBlockHeightPenalty {
			t.Errorf("expected WrongChildBlockHeight penalty, got %s", peerPenalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two connected headers with wrong difficulty
	h2.Number = big.NewInt(2)        // Child number 2, parent number 1
	h2.Difficulty = big.NewInt(2000) // Expected difficulty 10 + 1000 = 1010
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h1, &h2}, peer); err == nil {
		if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != WrongChildDifficultyPenalty {
			t.Errorf("expected WrongChildDifficulty penalty, got %s", peerPenalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two headers connected to the third header
	h2.Difficulty = big.NewInt(1010) // Fix difficulty of h2
	var h3 types.Header
	h3.Number = big.NewInt(2)
	h3.Difficulty = big.NewInt(1010)
	h3.ParentHash = h1.Hash()
	h3.Extra = []byte("I'm different") // To make sure the hash of h3 is different from the hash of h2
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h1, &h2, &h3}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].headers) != 3 {
			t.Errorf("expected chainSegment of the length 3, got %d", len(chainSegments[0].headers))
		}
		if chainSegments[0].headers[0] != &h1 {
			t.Errorf("expected h1 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Same three headers, but in a reverse order
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h3, &h2, &h1}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].headers) != 3 {
			t.Errorf("expected chainSegment of the length 3, got %d", len(chainSegments[0].headers))
		}
		if chainSegments[0].headers[0] != &h1 {
			t.Errorf("expected h1 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two headers not connected to each other
	if chainSegments, peerPenalty, err := hd.HandleHeadersMsg([]*types.Header{&h3, &h2}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 2 {
			t.Errorf("expected 2 chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}
}

func TestHandleNewBlockMsg(t *testing.T) {
	hd := NewHeaderDownload("", 10, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	})
	peer := PeerHandle(1)
	var h types.Header
	h.Number = big.NewInt(5)
	if chainSegments, peerPenalty, err := hd.HandleNewBlockMsg(&h, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].headers) != 1 {
			t.Errorf("expected chainSegment of the length 1, got %d", len(chainSegments[0].headers))
		}
		if chainSegments[0].headers[0] != &h {
			t.Errorf("expected h to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Same header with a bad hash
	hd.badHeaders[h.Hash()] = struct{}{}
	if chainSegments, peerPenalty, err := hd.HandleNewBlockMsg(&h, peer); err == nil {
		if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != BadBlockPenalty {
			t.Errorf("expected BadBlock penalty, got %s", peerPenalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle newBlock msg: %v", err)
	}
}

func TestPrepend(t *testing.T) {
	hd := NewHeaderDownload("", 10, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	})
	peer := PeerHandle(1)
	// empty chain segment
	if ok, peerPenalty, err := hd.Prepend(&ChainSegment{}, peer); err == nil {
		if peerPenalty != nil {
			t.Errorf("unexpected penalty: %s", peerPenalty)
		}
		if ok {
			t.Errorf("did not expect to prepend")
		}
	} else {
		t.Errorf("preprend: %v", err)
	}
}
