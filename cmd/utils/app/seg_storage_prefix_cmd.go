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

package app

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/urfave/cli/v3"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// storagePrefixStats estimates replacing the 20-byte address prefix of every
// storage key by the shortest prefix that is unique among the account keys of
// the same step range.
type storagePrefixStats struct {
	Accounts             uint64
	AccountsWithStorage  uint64
	StorageKeys          uint64
	UnmatchedStorageKeys uint64 // address is absent from the accounts file of the range
	UnmatchedAddresses   uint64
	SavedBytes           uint64
	UniformSavedBytes    uint64 // saving when each range uses its max prefix length, so no per-account length is stored
	MaxPrefixLen         int
	AccountsByPrefixLen  [length.Addr + 1]uint64
	SlotsByPrefixLen     [length.Addr + 1]uint64
}

// NetSavedBytes charges one byte per account for storing its prefix length.
func (s *storagePrefixStats) NetSavedBytes() int64 {
	return int64(s.SavedBytes) - int64(s.Accounts)
}

func (s *storagePrefixStats) add(o *storagePrefixStats) {
	s.Accounts += o.Accounts
	s.AccountsWithStorage += o.AccountsWithStorage
	s.StorageKeys += o.StorageKeys
	s.UnmatchedStorageKeys += o.UnmatchedStorageKeys
	s.UnmatchedAddresses += o.UnmatchedAddresses
	s.SavedBytes += o.SavedBytes
	s.UniformSavedBytes += o.UniformSavedBytes
	s.MaxPrefixLen = max(s.MaxPrefixLen, o.MaxPrefixLen)
	for i := range s.AccountsByPrefixLen {
		s.AccountsByPrefixLen[i] += o.AccountsByPrefixLen[i]
		s.SlotsByPrefixLen[i] += o.SlotsByPrefixLen[i]
	}
}

func commonPrefixLen(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// estimateStoragePrefix merge-joins the sorted keys of an accounts file with the
// sorted keys of the storage file of the same range. onStorageKey, if not nil,
// gets every storage key with the prefix length its address would use, or 0
// when the address has no account in the range.
func estimateStoragePrefix(accounts, storage *seg.Reader, onStorageKey func(k []byte, prefixLen int) error) (storagePrefixStats, error) {
	var s storagePrefixStats

	// Next returns either a slice of the read-only mapping or decodes into the
	// passed buffer, so decoded buffers are never reassigned from its result.
	accDec, stDec := make([]byte, 0, 256), make([]byte, 0, 256)

	nextAccount := func(buf []byte) ([]byte, bool, error) {
		if !accounts.HasNext() {
			return buf, false, nil
		}
		k, _ := accounts.Next(accDec[:0])
		if len(k) != length.Addr {
			return buf, false, fmt.Errorf("%s: unexpected account key length %d", accounts.FileName(), len(k))
		}
		buf = append(buf[:0], k...)
		if accounts.HasNext() {
			accounts.Skip()
		}
		return buf, true, nil
	}

	var sk []byte
	hasStorage := false
	nextStorage := func() error {
		hasStorage = storage.HasNext()
		if !hasStorage {
			return nil
		}
		sk, _ = storage.Next(stDec[:0])
		if len(sk) < length.Addr {
			return fmt.Errorf("%s: unexpected storage key length %d", storage.FileName(), len(sk))
		}
		if storage.HasNext() {
			storage.Skip()
		}
		return nil
	}

	var lastUnmatched []byte
	consumeUnmatchedBelow := func(account []byte) error {
		for hasStorage && (account == nil || bytes.Compare(sk[:length.Addr], account) < 0) {
			s.StorageKeys++
			s.UnmatchedStorageKeys++
			if lastUnmatched == nil || !bytes.Equal(lastUnmatched, sk[:length.Addr]) {
				s.UnmatchedAddresses++
				lastUnmatched = append(lastUnmatched[:0], sk[:length.Addr]...)
			}
			if onStorageKey != nil {
				if err := onStorageKey(sk, 0); err != nil {
					return err
				}
			}
			if err := nextStorage(); err != nil {
				return err
			}
		}
		return nil
	}

	if err := nextStorage(); err != nil {
		return s, err
	}
	cur, ok, err := nextAccount(nil)
	if err != nil {
		return s, err
	}
	next, hasNext, err := nextAccount(nil)
	if err != nil {
		return s, err
	}
	lcpPrev := 0
	for ok {
		lcpNext := 0
		if hasNext {
			lcpNext = commonPrefixLen(cur, next)
		}
		prefixLen := min(max(lcpPrev, lcpNext)+1, length.Addr)
		s.Accounts++
		s.AccountsByPrefixLen[prefixLen]++
		s.MaxPrefixLen = max(s.MaxPrefixLen, prefixLen)

		if err := consumeUnmatchedBelow(cur); err != nil {
			return s, err
		}
		var slots uint64
		for hasStorage && bytes.Equal(sk[:length.Addr], cur) {
			slots++
			if onStorageKey != nil {
				if err := onStorageKey(sk, prefixLen); err != nil {
					return s, err
				}
			}
			if err := nextStorage(); err != nil {
				return s, err
			}
		}
		if slots > 0 {
			s.AccountsWithStorage++
			s.StorageKeys += slots
			s.SlotsByPrefixLen[prefixLen] += slots
			s.SavedBytes += slots * uint64(length.Addr-prefixLen)
		}

		lcpPrev = lcpNext
		cur, next, ok = next, cur, hasNext
		if ok {
			if next, hasNext, err = nextAccount(next); err != nil {
				return s, err
			}
		}
	}
	if err := consumeUnmatchedBelow(nil); err != nil {
		return s, err
	}
	s.UniformSavedBytes = (s.StorageKeys - s.UnmatchedStorageKeys) * uint64(length.Addr-s.MaxPrefixLen)
	return s, nil
}

type storagePrefixRange struct {
	From, To            uint64
	Accounts, Storage   string
	Stats               storagePrefixStats
	KeysCompressedSize  int64 // storage keys compressed as they are; set only with recompression
	ShortCompressedSize int64 // storage keys compressed with shortened prefixes
}

// storagePrefixFilePairs returns accounts and storage .kv files of the same step
// range, skipping ranges covered by a bigger range and preferring the newest file version.
func storagePrefixFilePairs(domainDir string) ([]storagePrefixRange, error) {
	entries, err := os.ReadDir(domainDir)
	if err != nil {
		return nil, err
	}
	var files []snaptype.FileInfo
	for _, e := range entries {
		if f, _, ok := snaptype.ParseFileName(domainDir, e.Name()); ok && f.Ext == ".kv" {
			files = append(files, f)
		}
	}
	slices.SortFunc(files, func(a, b snaptype.FileInfo) int { return a.Version.Cmp(b.Version) })
	type stepRange struct{ from, to uint64 }
	accounts, storage := map[stepRange]string{}, map[stepRange]string{}
	for i := range files { // the newest version wins
		f := &files[i]
		switch f.TypeString {
		case kv.AccountsDomain.String():
			accounts[stepRange{f.From, f.To}] = f.Path
		case kv.StorageDomain.String():
			storage[stepRange{f.From, f.To}] = f.Path
		}
	}
	var res []storagePrefixRange
	for r, sPath := range storage {
		aPath, ok := accounts[r]
		if !ok {
			log.Warn("[storage-prefix] no accounts file for range", "file", filepath.Base(sPath))
			continue
		}
		res = append(res, storagePrefixRange{From: r.from, To: r.to, Accounts: aPath, Storage: sPath})
	}
	res = slices.DeleteFunc(res, func(a storagePrefixRange) bool {
		return slices.ContainsFunc(res, func(b storagePrefixRange) bool {
			return b.From <= a.From && a.To <= b.To && (b.From != a.From || b.To != a.To)
		})
	})
	slices.SortFunc(res, func(a, b storagePrefixRange) int { return cmp.Compare(a.From, b.From) })
	return res, nil
}

// estimateStoragePrefixFiles estimates every range of domainDir. With tmpDir set it
// also compresses storage keys with and without shortened prefixes, because
// the dictionary compression of storage keys already reduces repeated addresses.
func estimateStoragePrefixFiles(ctx context.Context, domainDir, tmpDir string, workers int, logger log.Logger) ([]storagePrefixRange, error) {
	ranges, err := storagePrefixFilePairs(domainDir)
	if err != nil {
		return nil, err
	}
	for i := range ranges {
		r := &ranges[i]
		logger.Info("[storage-prefix] processing", "accounts", filepath.Base(r.Accounts), "storage", filepath.Base(r.Storage))
		if err := estimateStoragePrefixRange(ctx, r, tmpDir, workers, logger); err != nil {
			return nil, err
		}
	}
	return ranges, nil
}

func estimateStoragePrefixRange(ctx context.Context, r *storagePrefixRange, tmpDir string, workers int, logger log.Logger) error {
	accD, err := seg.NewDecompressor(r.Accounts)
	if err != nil {
		return err
	}
	defer accD.Close()
	stD, err := seg.NewDecompressor(r.Storage)
	if err != nil {
		return err
	}
	defer stD.Close()
	accR := seg.NewReader(accD.MakeGetter(), statecfg.Schema.AccountsDomain.Compression)
	stR := seg.NewReader(stD.MakeGetter(), statecfg.Schema.StorageDomain.Compression)

	if tmpDir == "" {
		r.Stats, err = estimateStoragePrefix(accR, stR, nil)
		return err
	}

	cfg := statecfg.DomainCompressCfg
	cfg.Workers = workers
	newCompressor := func(name string) (*seg.Compressor, error) {
		c, err := seg.NewCompressor(ctx, "storage-prefix", filepath.Join(tmpDir, name), tmpDir, cfg, log.LvlDebug, logger)
		if err != nil {
			return nil, err
		}
		c.DisableFsync()
		return c, nil
	}
	keysC, err := newCompressor("keys.seg")
	if err != nil {
		return err
	}
	defer keysC.Close()
	shortC, err := newCompressor("short-keys.seg")
	if err != nil {
		return err
	}
	defer shortC.Close()

	var short []byte
	r.Stats, err = estimateStoragePrefix(accR, stR, func(k []byte, prefixLen int) error {
		if err := keysC.AddWord(k); err != nil {
			return err
		}
		if prefixLen == 0 {
			return shortC.AddWord(k)
		}
		short = append(append(short[:0], k[:prefixLen]...), k[length.Addr:]...)
		return shortC.AddWord(short)
	})
	if err != nil {
		return err
	}
	if r.KeysCompressedSize, err = compressedSize(keysC, filepath.Join(tmpDir, "keys.seg")); err != nil {
		return err
	}
	r.ShortCompressedSize, err = compressedSize(shortC, filepath.Join(tmpDir, "short-keys.seg"))
	return err
}

func compressedSize(c *seg.Compressor, path string) (int64, error) {
	if err := c.Compress(); err != nil {
		return 0, err
	}
	defer func() { _ = dir.RemoveFile(path) }()
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func segStoragePrefix(ctx context.Context, cliCtx *cli.Command) error {
	logger := log.Root()
	dirs := datadir.Open(cliCtx.String("datadir"))
	tmpDir := ""
	if cliCtx.Bool("recompress") {
		if err := os.MkdirAll(dirs.Tmp, 0o755); err != nil {
			return err
		}
		var err error
		if tmpDir, err = os.MkdirTemp(dirs.Tmp, "storage-prefix-"); err != nil {
			return err
		}
		defer func() { _ = dir.RemoveAll(tmpDir) }()
	}
	ranges, err := estimateStoragePrefixFiles(ctx, dirs.SnapDomain, tmpDir, estimate.CompressSnapshot.Workers(), logger)
	if err != nil {
		return err
	}
	printStoragePrefix(ranges, tmpDir != "")
	return nil
}

func printStoragePrefix(ranges []storagePrefixRange, recompressed bool) {
	p := message.NewPrinter(language.English)
	var total storagePrefixStats
	var totalKeysC, totalShortC int64
	for i := range ranges {
		r := &ranges[i]
		total.add(&r.Stats)
		totalKeysC += r.KeysCompressedSize
		totalShortC += r.ShortCompressedSize
		printStoragePrefixStats(p, fmt.Sprintf("steps %d-%d", r.From, r.To), &r.Stats, recompressed, r.KeysCompressedSize, r.ShortCompressedSize)
	}
	printStoragePrefixStats(p, "total", &total, recompressed, totalKeysC, totalShortC)
}

func printStoragePrefixStats(p *message.Printer, title string, s *storagePrefixStats, recompressed bool, keysC, shortC int64) {
	p.Printf("\n%s\n", title)
	p.Printf("  accounts: %d, with storage: %d\n", s.Accounts, s.AccountsWithStorage)
	p.Printf("  storage keys: %d, without account in range: %d (%d addresses)\n", s.StorageKeys, s.UnmatchedStorageKeys, s.UnmatchedAddresses)
	p.Printf("  raw address bytes of storage keys: %d\n", s.StorageKeys*length.Addr)
	p.Printf("  saved raw bytes: %d, minus 1 byte per account: %d\n", s.SavedBytes, s.NetSavedBytes())
	p.Printf("  saved raw bytes with one prefix length per range (max %d): %d\n", s.MaxPrefixLen, s.UniformSavedBytes)
	if recompressed {
		p.Printf("  compressed storage keys: %d, with short prefixes: %d, saved: %d, minus 1 byte per account: %d\n",
			keysC, shortC, keysC-shortC, keysC-shortC-int64(s.Accounts))
	}
	p.Printf("  prefix len: accounts / storage keys\n")
	for l := 1; l <= length.Addr; l++ {
		if s.AccountsByPrefixLen[l] == 0 {
			continue
		}
		p.Printf("    %2d: %d / %d\n", l, s.AccountsByPrefixLen[l], s.SlotsByPrefixLen[l])
	}
}
