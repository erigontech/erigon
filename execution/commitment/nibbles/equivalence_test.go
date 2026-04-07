package nibbles_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// TestEquivalence_HexToCompact_vs_HexNibblesToCompactBytes proves that the new
// nibbles.HexToCompact produces byte-identical output to the old
// commitment.HexNibblesToCompactBytes across a large random corpus.
func TestEquivalence_HexToCompact_vs_HexNibblesToCompactBytes(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))

	for i := 0; i < 500; i++ {
		l := rng.Intn(129) // length 0..128
		hex := make([]byte, l)
		for j := range hex {
			hex[j] = byte(rng.Intn(16))
		}
		// half with terminator
		if i%2 == 0 {
			hex = append(hex, nibbles.Terminator)
		}

		got := nibbles.HexToCompact(hex)
		want := commitment.HexNibblesToCompactBytes(hex)

		if !bytes.Equal(got, want) {
			t.Fatalf("HexToCompact mismatch for input %x:\n  nibbles=%x\n  commitment=%x", hex, got, want)
		}
	}
}

// TestEquivalence_CompactToHex_vs_uncompactNibbles proves that the new
// nibbles.CompactToHex produces byte-identical output to the old
// commitment.uncompactNibbles (exposed via temporary helper).
func TestEquivalence_CompactToHex_vs_uncompactNibbles(t *testing.T) {
	rng := rand.New(rand.NewSource(54321))

	for i := 0; i < 500; i++ {
		l := rng.Intn(129)
		hex := make([]byte, l)
		for j := range hex {
			hex[j] = byte(rng.Intn(16))
		}
		if i%2 == 0 {
			hex = append(hex, nibbles.Terminator)
		}

		compact := nibbles.HexToCompact(hex)

		got := nibbles.CompactToHex(compact)
		want := commitment.UncompactNibblesExported(compact)

		if !bytes.Equal(got, want) {
			t.Fatalf("CompactToHex mismatch for compact %x:\n  nibbles=%x\n  commitment=%x\n  (original hex=%x)", compact, got, want, hex)
		}
	}
}

// TestEquivalence_KeybytesToHex_vs_trie_keybytesToHex proves equivalence
// between nibbles.KeybytesToHex and trie.keybytesToHex (exposed via temporary helper).
func TestEquivalence_KeybytesToHex_vs_trie_keybytesToHex(t *testing.T) {
	rng := rand.New(rand.NewSource(99999))

	for i := 0; i < 500; i++ {
		l := rng.Intn(65) // length 0..64
		key := make([]byte, l)
		for j := range key {
			key[j] = byte(rng.Intn(256))
		}

		got := nibbles.KeybytesToHex(key)
		want := trie.KeybytesToHexExported(key)

		if !bytes.Equal(got, want) {
			t.Fatalf("KeybytesToHex mismatch for key %x:\n  nibbles=%x\n  trie=%x", key, got, want)
		}
	}
}

// TestEquivalence_HexToKeybytes_vs_trie_hexToKeybytes proves equivalence
// between nibbles.HexToKeybytes and trie.hexToKeybytes (exposed via temporary helper).
func TestEquivalence_HexToKeybytes_vs_trie_hexToKeybytes(t *testing.T) {
	rng := rand.New(rand.NewSource(77777))

	for i := 0; i < 500; i++ {
		// Generate valid hex sequences (even length for HexToKeybytes).
		l := rng.Intn(33) * 2 // even length 0..64
		hex := make([]byte, l)
		for j := range hex {
			hex[j] = byte(rng.Intn(16))
		}

		got := nibbles.HexToKeybytes(hex)
		want := trie.HexToKeybytesExported(hex)

		if !bytes.Equal(got, want) {
			t.Fatalf("HexToKeybytes mismatch for hex %x:\n  nibbles=%x\n  trie=%x", hex, got, want)
		}
	}
}
