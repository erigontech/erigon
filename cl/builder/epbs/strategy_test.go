package epbs

import (
	"math"
	"math/big"
	"testing"
)

func TestFixedMarginStrategy_ZeroBlockValue(t *testing.T) {
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, big.NewInt(0)); bid != nil {
		t.Fatalf("expected nil for zero block value, got %v", bid)
	}
}

func TestFixedMarginStrategy_NilBlockValue(t *testing.T) {
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, nil); bid != nil {
		t.Fatalf("expected nil for nil block value, got %v", bid)
	}
}

func TestFixedMarginStrategy_NegativeBlockValue(t *testing.T) {
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, big.NewInt(-100)); bid != nil {
		t.Fatalf("expected nil for negative block value, got %v", bid)
	}
}

func TestFixedMarginStrategy_NegativeProfit(t *testing.T) {
	// blockValue=100, margin=0.85 → bid=85, profit=15
	// MinProfit=20 → profit < MinProfit → skip
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(20)}
	if bid := s.Decide(1, big.NewInt(100)); bid != nil {
		t.Fatalf("expected nil when profit below minimum, got %v", bid)
	}
}

func TestFixedMarginStrategy_ExactThreshold(t *testing.T) {
	// blockValue=100, margin=0.85 → bid=85, profit=15
	// MinProfit=15 → profit == MinProfit → should bid
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(15)}
	bid := s.Decide(1, big.NewInt(100))
	if bid == nil {
		t.Fatal("expected bid at exact profit threshold, got nil")
	}
	if bid.Cmp(big.NewInt(85)) != 0 {
		t.Fatalf("expected bid 85, got %v", bid)
	}
}

func TestFixedMarginStrategy_NormalBid(t *testing.T) {
	// blockValue=1000, margin=0.85 → bid=850, profit=150
	// MinProfit=100 → profit >= MinProfit → bid
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(100)}
	bid := s.Decide(42, big.NewInt(1000))
	if bid == nil {
		t.Fatal("expected bid, got nil")
	}
	if bid.Cmp(big.NewInt(850)) != 0 {
		t.Fatalf("expected bid 850, got %v", bid)
	}
}

func TestFixedMarginStrategy_NilMinProfit(t *testing.T) {
	// MinProfit nil → no minimum check, always bid
	s := &FixedMarginStrategy{Margin: 0.99}
	bid := s.Decide(1, big.NewInt(1000))
	if bid == nil {
		t.Fatal("expected bid with nil MinProfit, got nil")
	}
	if bid.Cmp(big.NewInt(990)) != 0 {
		t.Fatalf("expected bid 990, got %v", bid)
	}
}

func TestFixedMarginStrategy_NegativeMargin(t *testing.T) {
	s := &FixedMarginStrategy{Margin: -0.1, MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, big.NewInt(100)); bid != nil {
		t.Fatalf("expected nil for negative margin, got %v", bid)
	}
}

func TestFixedMarginStrategy_MarginAboveOne(t *testing.T) {
	s := &FixedMarginStrategy{Margin: 1.5, MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, big.NewInt(100)); bid != nil {
		t.Fatalf("expected nil for margin > 1, got %v", bid)
	}
}

func TestFixedMarginStrategy_MarginNaN(t *testing.T) {
	s := &FixedMarginStrategy{Margin: math.NaN(), MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, big.NewInt(100)); bid != nil {
		t.Fatalf("expected nil for NaN margin, got %v", bid)
	}
}

func TestFixedMarginStrategy_MarginInf(t *testing.T) {
	s := &FixedMarginStrategy{Margin: math.Inf(1), MinProfit: big.NewInt(0)}
	if bid := s.Decide(1, big.NewInt(100)); bid != nil {
		t.Fatalf("expected nil for +Inf margin, got %v", bid)
	}
}

func TestFixedMarginStrategy_LargeBlockValue(t *testing.T) {
	// 10 ETH in wei
	blockValue := new(big.Int).Mul(big.NewInt(10), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	s := &FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(0)}
	bid := s.Decide(1, blockValue)
	if bid == nil {
		t.Fatal("expected bid for large block value, got nil")
	}

	// expected bid = 8.5 ETH
	expected := new(big.Int).Mul(big.NewInt(85), new(big.Int).Exp(big.NewInt(10), big.NewInt(17), nil))
	if bid.Cmp(expected) != 0 {
		t.Fatalf("expected bid %v, got %v", expected, bid)
	}
}
