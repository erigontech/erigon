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

package v3

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

// keySetBlock is one block of a key-set file: the pre-state it touches and the post-state it writes.
type keySetBlock struct {
	label     string
	pre, post []commitmenttest.Op
}

// readKeySet parses a key-set file. Lines: "B label" starts a block; "a"/"A" addr nonce balanceHex codeHash is an account
// before/after the block; "s"/"S" addr+slot valueHex is a slot before/after (empty or zero value after = delete); "X addr" deletes an account.
func readKeySet(path string) ([]keySetBlock, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var blocks []keySetBlock
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for n := 1; sc.Scan(); n++ {
		fs := strings.Fields(sc.Text())
		if len(fs) == 0 {
			continue
		}
		if fs[0] == "B" {
			blocks = append(blocks, keySetBlock{label: strings.Join(fs[1:], "_")})
			continue
		}
		if len(blocks) == 0 {
			return nil, fmt.Errorf("%s:%d: op before the first B line", path, n)
		}
		op, err := keySetOp(fs)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, n, err)
		}
		blk := &blocks[len(blocks)-1]
		if fs[0] == "a" || fs[0] == "s" {
			blk.pre = append(blk.pre, op)
		} else {
			blk.post = append(blk.post, op)
		}
	}
	return blocks, sc.Err()
}

func keySetOp(fs []string) (commitmenttest.Op, error) {
	key, err := hex.DecodeString(strings.TrimPrefix(fs[1], "0x"))
	if err != nil {
		return commitmenttest.Op{}, err
	}
	switch fs[0] {
	case "a", "A":
		if len(fs) != 5 || len(key) != 20 {
			return commitmenttest.Op{}, fmt.Errorf("bad account line %q", strings.Join(fs, " "))
		}
		nonce, err := strconv.ParseUint(fs[2], 10, 64)
		if err != nil {
			return commitmenttest.Op{}, err
		}
		v := commitmenttest.Account(commitmenttest.AccountSpec{Nonce: nonce, CodeHash: common.HexToHash(fs[4])})
		bal := strings.TrimLeft(strings.TrimPrefix(fs[3], "0x"), "0")
		if bal == "" {
			bal = "0"
		}
		if err := v.Balance.SetFromHex("0x" + bal); err != nil {
			return commitmenttest.Op{}, err
		}
		return commitmenttest.Op{Key: key, Account: &v}, nil
	case "s", "S":
		if len(key) != 52 {
			return commitmenttest.Op{}, fmt.Errorf("bad slot key %q", fs[1])
		}
		var val []byte
		if len(fs) > 2 {
			h := strings.TrimLeft(strings.TrimPrefix(fs[2], "0x"), "0")
			if len(h)%2 == 1 {
				h = "0" + h
			}
			if val, err = hex.DecodeString(h); err != nil {
				return commitmenttest.Op{}, err
			}
		}
		if len(val) == 0 {
			return commitmenttest.DeleteAt(key), nil
		}
		return commitmenttest.Op{Key: key, Storage: val}, nil
	case "X":
		return commitmenttest.DeleteAt(key), nil
	}
	return commitmenttest.Op{}, fmt.Errorf("unknown op %q", fs[0])
}

// BenchmarkV3KeySet folds each block of the key-set file V3_KEYSET over a trie seeded with V3_KEYSET_BG background
// accounts (default 1M) plus the block's pre-state; V3_WORKERS sets the scheduler width (0 = default).
// Without V3_KEYSET it runs nothing.
func BenchmarkV3KeySet(b *testing.B) {
	path := os.Getenv("V3_KEYSET")
	if path == "" {
		return
	}
	blocks, err := readKeySet(path)
	if err != nil {
		b.Fatal(err)
	}
	bg, workers := 1_000_000, 0
	if s := os.Getenv("V3_KEYSET_BG"); s != "" {
		if bg, err = strconv.Atoi(s); err != nil {
			b.Fatal(err)
		}
	}
	if s := os.Getenv("V3_WORKERS"); s != "" {
		if workers, err = strconv.Atoi(s); err != nil {
			b.Fatal(err)
		}
	}
	ctx := context.Background()
	background := make([]commitmenttest.Op, 0, bg)
	for i := range bg {
		background = append(background, parityAccount(benchAddr(i), i))
	}
	for _, blk := range blocks {
		b.Run(fmt.Sprintf("%s/keys=%d", blk.label, len(blk.post)), func(b *testing.B) {
			seed := append(append([]commitmenttest.Op(nil), background...), blk.pre...)
			base := newShardedContext()
			tr := newFactoryTrie(workers, base, base.factory)
			if _, err := tr.Process(ctx, benchUpdatesIn(b.TempDir(), commitment.ModeCollect, seed, updatesOf(seed)), "", nil, commitment.WarmupConfig{}); err != nil {
				b.Fatal(err)
			}
			tr.Release()
			dir := b.TempDir()
			postUpdates := updatesOf(blk.post)
			b.ReportAllocs()
			b.ResetTimer()
			timedProcess(b, func() (func() error, func()) {
				c := newShardedContext()
				for s := range base.shards {
					c.shards[s].branches = maps.Clone(base.shards[s].branches)
				}
				tr := newFactoryTrie(workers, c, c.factory)
				u := benchUpdatesIn(dir, commitment.ModeCollect, blk.post, postUpdates)
				return func() error {
					_, err := tr.Process(ctx, u, "", nil, commitment.WarmupConfig{})
					return err
				}, tr.Release
			})
		})
	}
}

func TestReadKeySet(t *testing.T) {
	path := t.TempDir() + "/ks.txt"
	addr := strings.Repeat("ab", 20)
	slot := addr + strings.Repeat("01", 32)
	body := "B blk 1\n" +
		"a " + addr + " 1 0x00ff " + strings.Repeat("00", 32) + "\n" +
		"A " + addr + " 2 0x100 " + strings.Repeat("00", 32) + "\n" +
		"s " + slot + " 0x0007\n" +
		"S " + slot + " 0x0\n" +
		"B blk2\nX " + addr + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	blocks, err := readKeySet(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 || blocks[0].label != "blk_1" || len(blocks[0].pre) != 2 || len(blocks[0].post) != 2 || len(blocks[1].post) != 1 {
		t.Fatalf("unexpected blocks %+v", blocks)
	}
	if a := blocks[0].post[0].Account; a == nil || a.Nonce != 2 || a.Balance.Uint64() != 0x100 {
		t.Fatalf("bad post account %+v", a)
	}
	if s := blocks[0].pre[1].Storage; len(s) != 1 || s[0] != 7 {
		t.Fatalf("bad pre slot %x", s)
	}
	if !blocks[0].post[1].Delete || !blocks[1].post[0].Delete {
		t.Fatal("zero slot and X must delete")
	}
}
