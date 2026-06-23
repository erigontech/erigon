package eliasfano16

import "testing"

// oversparseEF builds an EliasFano whose first superQ block spans more than
// 1<<16 bits: with minDelta=0 and l=0 element c lands at bit 5*c, so the jump
// offset first exceeds the 16-bit slot at the q=256 boundary c=13312.
func oversparseEF(n int) *EliasFano {
	keys := make([]uint64, n+1)
	var minDelta uint64
	for i := range n {
		var b uint64
		if i < 16000 {
			b = 4
		}
		keys[i+1] = keys[i] + b
		if i == 0 || b < minDelta {
			minDelta = b
		}
	}
	ef := NewEliasFano(uint64(n+1), keys[n], minDelta)
	for _, c := range keys {
		ef.AddOffset(c)
	}
	return ef
}

func TestBuildRejectsOversparseSequence(t *testing.T) {
	if oversparseEF(64002).build() {
		t.Fatal("build() must return false when a jump offset exceeds 16 bits")
	}
}

func TestBuildPanicsOnOversparseSequence(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Build() must panic on a sequence too sparse for 16-bit jumps")
		}
	}()
	oversparseEF(64002).Build()
}

func TestBuildAcceptsDenseSequence(t *testing.T) {
	const n = 70000 // > 2*superQ, so superQ jump logic runs; offsets stay within 16 bits
	keys := make([]uint64, n+1)
	for i := range n {
		keys[i+1] = keys[i] + 1
	}
	ef := NewEliasFano(uint64(n+1), keys[n], 1)
	for _, c := range keys {
		ef.AddOffset(c)
	}
	if !ef.build() {
		t.Fatal("build() must accept a dense in-domain sequence")
	}
	for i := range n {
		if got := ef.Get(uint64(i)); got != keys[i] {
			t.Fatalf("Get(%d) = %d, want %d", i, got, keys[i])
		}
	}
}

func TestDoubleBuildRejectsOversparseSequence(t *testing.T) {
	const n = 64002
	cumKeys := make([]uint64, n+1)
	position := make([]uint64, n+1)
	for i := range n {
		var d uint64
		if i < 16000 {
			d = 4 // oversparse cumKeys: overflows the 16-bit jump offset
		}
		cumKeys[i+1] = cumKeys[i] + d
		position[i+1] = position[i] + 1
	}
	var ef DoubleEliasFano
	if ef.build(cumKeys, position) {
		t.Fatal("DoubleEliasFano.build() must return false when a jump offset exceeds 16 bits")
	}
}

func TestDoubleBuildAcceptsDenseSequence(t *testing.T) {
	const n = 70000
	cumKeys := make([]uint64, n+1)
	position := make([]uint64, n+1)
	for i := range n {
		cumKeys[i+1] = cumKeys[i] + 1
		position[i+1] = position[i] + 2
	}
	var ef DoubleEliasFano
	if !ef.build(cumKeys, position) {
		t.Fatal("DoubleEliasFano.build() must accept a dense in-domain sequence")
	}
	for b := range n {
		cumKey, pos := ef.Get2(uint64(b))
		if cumKey != cumKeys[b] || pos != position[b] {
			t.Fatalf("Get2(%d) = (%d, %d), want (%d, %d)", b, cumKey, pos, cumKeys[b], position[b])
		}
	}
}
