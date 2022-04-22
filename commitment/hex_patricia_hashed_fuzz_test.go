//go:build gofuzzbeta
// +build gofuzzbeta

package commitment

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// gotip test -trimpath -v -tags gofuzzbeta -fuzz=Fuzz_ProcessUpdate$ -fuzztime=300s ./commitment

func Fuzz_ProcessUpdate(f *testing.F) {
	ha, _ := hex.DecodeString("13ccfe8074645cab4cb42b423625e055f0293c87")
	hb, _ := hex.DecodeString("73f822e709a0016bfaed8b5e81b5f86de31d6895")

	f.Add(uint64(2), ha, uint64(1235105), hb)

	f.Fuzz(func(t *testing.T, balanceA uint64, accountA []byte, balanceB uint64, accountB []byte) {
		if len(accountA) == 0 || len(accountA) > 20 || len(accountB) == 0 || len(accountB) > 20 {
			t.Skip()
		}

		builder := NewUpdateBuilder().
			Balance(hex.EncodeToString(accountA), balanceA).
			Balance(hex.EncodeToString(accountB), balanceB)

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(20, ms.branchFn, ms.accountFn, ms.storageFn)
		hphAnother := NewHexPatriciaHashed(20, ms2.branchFn, ms2.accountFn, ms2.storageFn)

		hph.SetTrace(false)
		hphAnother.SetTrace(false)

		plainKeys, hashedKeys, updates := builder.Build()
		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}
		if err := ms2.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		branchNodeUpdates, err := hph.ProcessUpdates(plainKeys, hashedKeys, updates)
		if err != nil {
			t.Fatal(err)
		}

		ms.applyBranchNodeUpdates(branchNodeUpdates)
		rootHash, err := hph.RootHash()
		if err != nil {
			t.Fatalf("failed to evaluate root hash: %v", err)
		}
		if len(rootHash) != 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}

		branchNodeUpdates, err = hphAnother.ProcessUpdates(plainKeys, hashedKeys, updates)
		if err != nil {
			t.Fatal(err)
		}
		ms2.applyBranchNodeUpdates(branchNodeUpdates)

		rootHashAnother, err := hphAnother.RootHash()
		if err != nil {
			t.Fatalf("failed to evaluate root hash: %v", err)
		}
		if len(rootHashAnother) > 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}
		if !bytes.Equal(rootHash, rootHashAnother) {
			t.Fatalf("invalid second root hash with same updates: [%v] != [%v]", hex.EncodeToString(rootHash), hex.EncodeToString(rootHashAnother))
		}
	})
}

// gotip test -trimpath -v -tags gofuzzbeta -fuzz=Fuzz_ProcessUpdates_ArbitraryUpdateCount -fuzztime=300s ./commitment

func Fuzz_ProcessUpdates_ArbitraryUpdateCount(f *testing.F) {
	ha, _ := hex.DecodeString("83a93c6ddd2660654f34d55f5deead039a4ac4853528b894383f646193852ddb078e00fbcb52d82bb791edddb1cffee89e599b5b45bb60f04b6c5c276635570c12e31d882f333b6beab06c11e603881b0c68788beca64fcc9185fb2823da72151d077192d321d83df17d49f2e37f2f69e43b147bc7bd8c3ae7ea161b7c9e81c5a540f37158e79f3d503813a32374abb0f94ad7d8ddca63bfd427e8570b64bb6e0b255e344f2e2849c623d6690c2d6ea66d90818e3169297acc58177cb3b8fae48852883b2850c7a48f4b0eea3ccc4c04e6cb6025e9e8f7db2589c7dae81517c514790cfd6f668903161349e")

	f.Add(ha)

	f.Fuzz(func(t *testing.T, build []byte) {
		keyMap := make(map[string]uint64)
		i := 0
		for i < len(build) {
			keyLen := int(build[i]>>16) + 1
			valLen := int(build[i]&15) + 1
			i++
			var key []byte
			var val uint64
			for keyLen > 0 && i < len(build) {
				key = append(key, build[i])
				i++
				keyLen--
			}
			for valLen > 0 && i < len(build) {
				val += uint64(build[i])
				i++
				valLen--
			}
			keyMap[hex.EncodeToString(key)] = val
		}

		builder := NewUpdateBuilder()
		for account, balance := range keyMap {
			builder.Balance(account, balance)
		}

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(20, ms.branchFn, ms.accountFn, ms.storageFn)
		hphAnother := NewHexPatriciaHashed(20, ms2.branchFn, ms2.accountFn, ms2.storageFn)

		hph.SetTrace(false)
		hphAnother.SetTrace(false)

		plainKeys, hashedKeys, updates := builder.Build()
		if err := ms.applyPlainUpdates(plainKeys, updates); err != nil {
			t.Fatal(err)
		}

		branchNodeUpdates, err := hph.ProcessUpdates(plainKeys, hashedKeys, updates)
		if err != nil {
			t.Fatal(err)
		}

		ms.applyBranchNodeUpdates(branchNodeUpdates)
		rootHash, err := hph.RootHash()
		if err != nil {
			t.Fatalf("failed to evaluate root hash: %v", err)
		}
		if len(rootHash) != 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}

		branchNodeUpdates, err = hphAnother.ProcessUpdates(plainKeys, hashedKeys, updates)
		if err != nil {
			t.Fatal(err)
		}

		rootHashAnother, err := hphAnother.RootHash()
		if err != nil {
			t.Fatalf("failed to evaluate root hash: %v", err)
		}
		if len(rootHashAnother) > 32 {
			t.Fatalf("invalid root hash length: expected 32 bytes, got %v", len(rootHash))
		}
		if !bytes.Equal(rootHash, rootHashAnother) {
			t.Fatalf("invalid second root hash with same updates: [%v] != [%v]", hex.EncodeToString(rootHash), hex.EncodeToString(rootHashAnother))
		}
	})
}
