package blockreplay

import (
	"maps"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// a synthetic single-block Fixture with the given witness cells.
func fxWith(accounts map[[20]byte]acctData, storage map[[20]byte]map[[32]byte][32]byte) *Fixture {
	fx := newFixture()
	maps.Copy(fx.Accounts, accounts)
	for a, slots := range storage {
		inner := map[[32]byte][32]byte{}
		maps.Copy(inner, slots)
		fx.Storage[a] = inner
	}
	return fx
}

func TestRangeMergedWitnessKeepsEarliest(t *testing.T) {
	t.Parallel()
	addr := [20]byte{0xaa}
	slot := [32]byte{0x01}

	early := fxWith(
		map[[20]byte]acctData{addr: {Present: true, Nonce: 1}},
		map[[20]byte]map[[32]byte][32]byte{addr: {slot: {0x11}}},
	)
	// second block reads the SAME key after the first block wrote it: its
	// recorded pre-value is the first block's post-value and must NOT overwrite
	// the true range-start value.
	late := fxWith(
		map[[20]byte]acctData{addr: {Present: true, Nonce: 2}},
		map[[20]byte]map[[32]byte][32]byte{addr: {slot: {0x22}}},
	)

	rf := &RangeFixture{Blocks: []*Fixture{early, late}}
	m := rf.MergedWitness()

	require.Equal(t, uint64(1), m.Accounts[addr].Nonce, "account: earliest wins")
	require.Equal(t, [32]byte{0x11}, m.Storage[addr][slot], "storage: earliest wins")
}

func TestRangeSaveLoadRoundTrip(t *testing.T) {
	t.Parallel()
	addr := [20]byte{0xbe}
	b0 := fxWith(map[[20]byte]acctData{addr: {Present: true, Nonce: 7}}, nil)
	b1 := fxWith(map[[20]byte]acctData{{0xcd}: {Present: true, Nonce: 9}}, nil)
	out := newOutputs()
	out.Accounts[addr] = acctData{Present: true, Nonce: 8}
	rf := &RangeFixture{Blocks: []*Fixture{b0, b1}, Outputs: out}

	path := filepath.Join(t.TempDir(), "range.gob")
	require.NoError(t, rf.Save(path))
	got, err := LoadRange(path)
	require.NoError(t, err)
	require.Len(t, got.Blocks, 2)
	require.Equal(t, uint64(7), got.Blocks[0].Accounts[addr].Nonce)
	require.Equal(t, uint64(8), got.Outputs.Accounts[addr].Nonce)
}

// TestRangeMergedWitnessDropsOrphans pins that the merged witness never carries storage
// or code for an address with no present account in the merge: SeedDomains seeds both only
// for a present account (seed.go), so an orphan entry would be written against an account
// that was never seeded and the replay then diverges. Both the account-absent (!ok) and the
// recorded-but-absent (Present:false, what the serial witness actually produces when a block
// reads a non-existent account) shapes must be pruned.
func TestRangeMergedWitnessDropsOrphans(t *testing.T) {
	t.Parallel()
	addr := [20]byte{0xbb}
	slot := [32]byte{0x01}

	cases := map[string]map[[20]byte]acctData{
		"account absent":      {},
		"account not present": {addr: {Present: false}},
	}
	for name, accts := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			orphan := fxWith(accts, map[[20]byte]map[[32]byte][32]byte{addr: {slot: {0x33}}})
			orphan.Code[addr] = []byte{0x60, 0x00}
			rf := &RangeFixture{Blocks: []*Fixture{orphan}}

			m := rf.MergedWitness()

			_, hasStorage := m.Storage[addr]
			require.False(t, hasStorage, "merged witness must drop storage for an account with no present record")
			_, hasCode := m.Code[addr]
			require.False(t, hasCode, "merged witness must drop code for an account with no present record")
		})
	}
}
