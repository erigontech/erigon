package headerdownload

import (
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

const TestBufferLimit = 32 * 1024
const TestTipLimit = 10
const TestInitPowDepth = 16

func TestSplitIntoSegments(t *testing.T) {
	hd := NewHeaderDownload(common.Hash{}, "", TestBufferLimit, TestTipLimit, TestInitPowDepth, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	}, nil, 60, 60)

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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}}, []*types.Header{&h}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}}, []*types.Header{&h, &h}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}}, []*types.Header{&h}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}}, []*types.Header{&h1, &h2}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}}, []*types.Header{&h1, &h2}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}}, []*types.Header{&h1, &h2}); err == nil {
		if penalty != WrongChildDifficultyPenalty {
			t.Errorf("expected WrongChildDifficulty penalty, got %s", penalty)
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}, []byte{}}, []*types.Header{&h1, &h2, &h3}); err == nil {
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

	// Same three headers, but in a reverse order
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}, []byte{}}, []*types.Header{&h3, &h2, &h1}); err == nil {
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
	if chainSegments, penalty, err := hd.SplitIntoSegments([][]byte{[]byte{}, []byte{}}, []*types.Header{&h3, &h2}); err == nil {
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
	hd := NewHeaderDownload(common.Hash{}, "", TestBufferLimit, TestTipLimit, TestInitPowDepth, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	}, nil, 60, 60)
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

func TestFindTip(t *testing.T) {
	/*
		hd := NewHeaderDownload("", 10, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
			// To get child difficulty, we just add 1000 to the parent difficulty
			return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
		}, func(header *types.Header) error {
			return nil
		},
		)

				// trying to attach header with wrong block height
				var h5 types.Header
				h5.Number = big.NewInt(6) // Wrong (expected 5)
				h5.Difficulty = big.NewInt(4010)
				h5.ParentHash = h4.Hash()
				if err := hd.ExtendUp(&ChainSegment{headers: []*types.Header{&h5}}, 0, 1); err == nil {
					if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != WrongChildBlockHeightPenalty {
						t.Errorf("expected WrongChildBlockHeight penalty, got %s", peerPenalty)
					}
					if ok {
						t.Errorf("did not expect to prepend")
					}
					if len(hd.tips) != 5 {
						t.Errorf("expected 5 tips, got %d", len(hd.tips))
					}
				} else {
					t.Errorf("prepend: %v", err)
				}

				// trying to attach header with wrong difficulty
				h5.Number = big.NewInt(5)        // Now correct
				h5.Difficulty = big.NewInt(4020) // Wrong - expected 4010
				if ok, peerPenalty, err := hd.Prepend(&ChainSegment{headers: []*types.Header{&h5}}, peer); err == nil {
					if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != WrongChildDifficultyPenalty {
						t.Errorf("expected WrongChildDifficulty penalty, got %s", peerPenalty)
					}
					if ok {
						t.Errorf("did not expect to prepend")
					}
					if len(hd.tips) != 5 {
						t.Errorf("expected 5 tips, got %d", len(hd.tips))
					}
				} else {
					t.Errorf("prepend: %v", err)
				}

				// trying to attach header with wrong PoW
				hd.verifySealFunc = func(header *types.Header) error {
					if header.Nonce.Uint64() > 0 {
						return fmt.Errorf("wrong nonce: %d", header.Nonce)
					}
					return nil
				}
			h5.Difficulty = big.NewInt(4010) // Now correct
			h5.Nonce = types.EncodeNonce(1)
			if ok, peerPenalty, err := hd.Prepend(&ChainSegment{headers: []*types.Header{&h5}}, peer); err == nil {
				if peerPenalty == nil || peerPenalty.peerHandle != peer || peerPenalty.penalty != InvalidSealPenalty {
					t.Errorf("expected InvalidSeal penalty, got %s", peerPenalty)
				}
				if ok {
					t.Errorf("did not expect to prepend")
				}
				if len(hd.tips) != 5 {
					t.Errorf("expected 5 tips, got %d", len(hd.tips))
				}
			} else {
				t.Errorf("prepend: %v", err)
			}
	*/
}

func TestExtendUp(t *testing.T) {
	hd := NewHeaderDownload(common.Hash{}, "", TestBufferLimit, TestTipLimit, TestInitPowDepth, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	}, func(header *types.Header) error {
		return nil
	}, 60, 60,
	)

	var currentTime uint64 = 100
	// single header in the chain segment
	var h types.Header
	if err := hd.ExtendUp(&ChainSegment{Headers: []*types.Header{&h}}, 0, 1, currentTime); err == nil {
		t.Errorf("extendUp without working tips - expected error")
	}

	// single header attaching to a single existing tip
	var h1, h2 types.Header
	h1.Number = big.NewInt(1)
	h1.Difficulty = big.NewInt(10)
	h2.Number = big.NewInt(2)
	h2.Difficulty = big.NewInt(1010)
	h2.ParentHash = h1.Hash()
	if anchor, err := hd.addHeaderAsAnchor(&h1, false /* hardCoded */); err == nil {
		if err1 := hd.addHeaderAsTip(&h1, anchor, *new(uint256.Int).SetUint64(2000), currentTime, false /* hardCodedTip */); err1 != nil {
			t.Fatalf("setting up h1 (tip): %v", err1)
		}
	} else {
		t.Errorf("setting up h1 (anchor): %v", err)
	}
	if err := hd.ExtendUp(&ChainSegment{Headers: []*types.Header{&h2}}, 0, 1, currentTime); err == nil {
		if len(hd.tips) != 2 {
			t.Errorf("expected 2 tips, got %d", len(hd.tips))
		}
	} else {
		t.Errorf("extendUp: %v", err)
	}

	// two connected headers attaching to the the highest tip
	var h3, h4 types.Header
	h3.Number = big.NewInt(3)
	h3.Difficulty = big.NewInt(2010)
	h3.ParentHash = h2.Hash()
	h4.Number = big.NewInt(4)
	h4.Difficulty = big.NewInt(3010)
	h4.ParentHash = h3.Hash()
	if err := hd.ExtendUp(&ChainSegment{Headers: []*types.Header{&h4, &h3}}, 0, 2, currentTime); err == nil {
		if len(hd.tips) != 4 {
			t.Errorf("expected 4 tips, got %d", len(hd.tips))
		}
		tip, ok := hd.getTip(h4.Hash())
		if !ok {
			t.Errorf("did not find h4 in the tips")
		}
		if ok && !tip.cumulativeDifficulty.Eq(new(uint256.Int).SetUint64(2000+1010+2010+3010)) {
			t.Errorf("cumulative difficulty of h4 expected %d, got %d", 2000+1010+2010+3010, tip.cumulativeDifficulty.ToBig())
		}
	} else {
		t.Errorf("extendUp: %v", err)
	}

	// one header attaching not to the highest tip
	var h41 types.Header
	h41.Number = big.NewInt(4)
	h41.Difficulty = big.NewInt(3010)
	h41.Extra = []byte("Extra")
	h41.ParentHash = h3.Hash()
	if err := hd.ExtendUp(&ChainSegment{Headers: []*types.Header{&h41}}, 0, 1, currentTime); err == nil {
		if len(hd.tips) != 5 {
			t.Errorf("expected 5 tips, got %d", len(hd.tips))
		}
		tip, ok := hd.getTip(h41.Hash())
		if !ok {
			t.Errorf("did not find h41 in the tips")
		}
		if ok && !tip.cumulativeDifficulty.Eq(new(uint256.Int).SetUint64(2000+1010+2010+3010)) {
			t.Errorf("cumulative difficulty of h41 expected %d, got %d", 2000+1010+2010+3010, tip.cumulativeDifficulty.ToBig())
		}
		if ok && tip.anchor.hash != h1.Hash() {
			t.Errorf("Expected h41 anchor to be %x, got %x", h1.Hash(), tip.anchor.hash)
		}
	} else {
		t.Errorf("extendUp: %v", err)
	}

	var h5 types.Header
	h5.Number = big.NewInt(5)
	h5.Difficulty = big.NewInt(4010)
	h5.ParentHash = h4.Hash()
	// trying to attach header not connected to any tips
	var h6 types.Header
	h6.Number = big.NewInt(6)
	h6.Difficulty = big.NewInt(5010)
	h6.ParentHash = h5.Hash()
	if err := hd.ExtendUp(&ChainSegment{Headers: []*types.Header{&h6}}, 0, 1, currentTime); err == nil {
		t.Errorf("extendUp not connected to tips - expected error")
	}

	// Introduce h5 as a tip and prepend h6
	if anchor, err := hd.addHeaderAsAnchor(&h5, false /* hardCoded */); err == nil {
		if err1 := hd.addHeaderAsTip(&h5, anchor, *new(uint256.Int).SetUint64(10000), currentTime, false /* hardCodedTip */); err1 != nil {
			t.Fatalf("setting up h5 (tip): %v", err1)
		}
	} else {
		t.Errorf("setting up h5 (anchor): %v", err)
	}
	if err := hd.ExtendUp(&ChainSegment{Headers: []*types.Header{&h6}}, 0, 1, currentTime); err == nil {
		if len(hd.tips) != 7 {
			t.Errorf("expected 7 tips, got %d", len(hd.tips))
		}
		tip, ok := hd.getTip(h6.Hash())
		if !ok {
			t.Errorf("did not find h6 in the tips")
		}
		if ok && !tip.cumulativeDifficulty.Eq(new(uint256.Int).SetUint64(10000+5010)) {
			t.Errorf("cumulative difficulty of h6 expected %d, got %d", 10000+5010, tip.cumulativeDifficulty.ToBig())
		}
		if ok && tip.anchor.hash != h5.Hash() {
			t.Errorf("Expected h6 anchor to be %x, got %x", h5.Hash(), tip.anchor.hash)
		}
	} else {
		t.Errorf("prepend: %v", err)
	}

	var h7 types.Header
	h7.Number = big.NewInt(7)
	h7.Difficulty = big.NewInt(6010)
	h7.ParentHash = common.HexToHash("0x4354543543959438594359348990345893408")
	// Introduce hard-coded tip
	if anchor, err := hd.addHeaderAsAnchor(&h7, true /* hardCoded */); err == nil {
		hd.addHardCodedTip(10, 5555, h7.Hash(), anchor, *new(uint256.Int).SetUint64(2000))
	} else {
		t.Fatalf("settings up h7 (anchor): %v", err)
	}
}

func TestExtendDown(t *testing.T) {
	hd := NewHeaderDownload(common.Hash{}, "", TestBufferLimit, TestTipLimit, TestInitPowDepth, func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		// To get child difficulty, we just add 1000 to the parent difficulty
		return big.NewInt(0).Add(parentDifficulty, big.NewInt(1000))
	}, func(header *types.Header) error {
		return nil
	}, 60, 60,
	)

	// single header in the chain segment
	var h types.Header
	if err := hd.ExtendDown(&ChainSegment{HeadersRaw: [][]byte{}, Headers: []*types.Header{&h}}, 0, 1, false /* hardCoded */, uint64(time.Now().Unix())); err == nil {
		t.Errorf("extendDown without working trees - expected error")
	}
}
