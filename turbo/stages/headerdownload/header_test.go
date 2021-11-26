package headerdownload

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
)

func newCSHeaders(headers ...*types.Header) []ChainSegmentHeader {
	csHeaders := make([]ChainSegmentHeader, 0, len(headers))
	for _, header := range headers {
		headerRaw, _ := rlp.EncodeToBytes(header)
		h := ChainSegmentHeader{
			HeaderRaw: headerRaw,
			Header:    header,
			Hash:      header.Hash(),
			Number:    header.Number.Uint64(),
		}
		csHeaders = append(csHeaders, h)
	}
	return csHeaders
}

func TestSplitIntoSegments(t *testing.T) {
	engine := ethash.NewFaker()
	hd := NewHeaderDownload(100, 100, engine)

	// Empty message
	if chainSegments, penalty, err := hd.SplitIntoSegments([]ChainSegmentHeader{}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h)); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h, &h)); err == nil {
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
	hd.ReportBadHeader(h.Hash())
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h)); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h1, &h2)); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0]) != 2 {
			t.Errorf("expected chainSegment of the length 2, got %d", len(chainSegments[0]))
		}
		if chainSegments[0][0].Header != &h2 {
			t.Errorf("expected h2 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two connected headers with wrong numbers
	h2.Number = big.NewInt(3) // Child number 3, parent number 1
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h1, &h2)); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h3, &h2, &h1)); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 3 {
			t.Errorf("expected 3 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0]) != 1 {
			t.Errorf("expected chainSegment of the length 1, got %d", len(chainSegments[0]))
		}
		if chainSegments[2][0].Header != &h1 {
			t.Errorf("expected h1 to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Two headers not connected to each other
	if chainSegments, penalty, err := hd.SplitIntoSegments(newCSHeaders(&h3, &h2)); err == nil {
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
	headerRaw, _ := rlp.EncodeToBytes(h)
	if chainSegments, penalty, err := hd.SingleHeaderAsSegment(headerRaw, &h); err == nil {
		if penalty != NoPenalty {
			t.Errorf("unexpected penalty: %s", penalty)
		}
		if len(chainSegments) != 1 {
			t.Errorf("expected 1 chainSegments, got %d", len(chainSegments))
		}
		if len(chainSegments[0]) != 1 {
			t.Errorf("expected chainSegment of the length 1, got %d", len(chainSegments[0]))
		}
		if chainSegments[0][0].Header != &h {
			t.Errorf("expected h to be the root")
		}
	} else {
		t.Errorf("handle header msg: %v", err)
	}

	// Same header with a bad hash
	hd.ReportBadHeader(h.Hash())
	if chainSegments, penalty, err := hd.SingleHeaderAsSegment(headerRaw, &h); err == nil {
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
