package headerdownload

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func TestSplitIntoSegments(t *testing.T) {
	engine := ethash.NewFaker()
	hd := NewHeaderDownload(100, 100, engine)

	// Empty message
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{}, []*types.Header{}); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}}, []*types.Header{&h}); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Same header repeated twice
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}, {}}, []*types.Header{&h, &h}); err == nil {
		if penalty != DuplicateHeaderPenalty {
			t.Errorf("expected DuplicateHeader penalty, got %s", penalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Single header with a bad hash
	hd.badHeaders[h.Hash()] = struct{}{}
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}}, []*types.Header{&h}); err == nil {
		if penalty != BadBlockPenalty {
			t.Errorf("expected BadBlock penalty, got %s", penalty)
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}, {}}, []*types.Header{&h1, &h2}); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].Headers) != 2 {
			t.Errorf("expected chainSegment of the length 2, got %d", len(chainSegments[0].Headers))
		}
		if chainSegments[0].Headers[0] != &h2 {
			t.Errorf("expected h2 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two connected headers with wrong numbers
	h2.Number = big.NewInt(3) // Child number 3, parent number 1
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}, {}}, []*types.Header{&h1, &h2}); err == nil {
		if penalty != WrongChildBlockHeightPenalty {
			t.Errorf("expected WrongChildBlockHeight penalty, got %s", penalty)
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

	// Two headers connected to the third header
	h2.Difficulty = big.NewInt(1010) // Fix difficulty of h2
	var h3 types.Header
	h3.Number = big.NewInt(2)
	h3.Difficulty = big.NewInt(1010)
	h3.ParentHash = h1.Hash()
	h3.Extra = []byte("I'm different") // To make sure the hash of h3 is different from the hash of h2

	// Same three headers, but in a reverse order
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}, {}, {}}, []*types.Header{&h3, &h2, &h1}); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 3 {
			t.Errorf("expected 3 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].Headers) != 1 {
			t.Errorf("expected chainSegment of the length 1, got %d", len(chainSegments[0].Headers))
		}
		if chainSegments[2].Headers[0] != &h1 {
			t.Errorf("expected h1 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two headers not connected to each other
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{{}, {}}, []*types.Header{&h3, &h2}); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 2 {
			t.Errorf("expected 2 chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}
}

func TestSingleHeaderAsSegment(t *testing.T) {
	engine := ethash.NewFaker()
	hd := NewHeaderDownload(100, 100, engine)
	var h types.Header
	h.Number = big.NewInt(5)
	if chainSegments, penalty, err := hd.SingleHeaderAsSegment([]byte{}, &h); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0].Headers) != 1 {
			t.Errorf("expected chainSegment of the length 1, got %d", len(chainSegments[0].Headers))
		}
		if chainSegments[0].Headers[0] != &h {
			t.Errorf("expected h to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Same header with a bad hash
	hd.badHeaders[h.Hash()] = struct{}{}
	if chainSegments, penalty, err := hd.SingleHeaderAsSegment([]byte{}, &h); err == nil {
		if penalty != BadBlockPenalty {
			t.Errorf("expected BadBlock penalty, got %s", penalty)
		}
		if chainSegments != nil {
			t.Errorf("expected no chainSegments, got %d", len(chainSegments))
		}
	} else {
		t.Errorf("handle newBlock msg: %v", err)
	}
}
