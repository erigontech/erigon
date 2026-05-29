// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package memstoredb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
)

// Persistence layer: a memstoredb path can opt into a single dump-file alongside
// it (`<path>.mem`). The file is written by saveToFile (called from Close) and
// read by loadFromFile (called from OpenForPath when the file exists).
//
// Rationale: erigon's CLI tooling (notably hive's wrapper) runs `erigon init` /
// `erigon import` / `erigon` as separate processes and expects chaindata to
// survive between them. A purely volatile in-memory backend breaks that — the
// genesis a prior `init` wrote is gone by the time `import` opens the DB and
// hits eth.New, which reads genesis from the DB and stores nil in cfg.Genesis;
// eth.Init then nil-derefs at ethCfg.Genesis.Config.
//
// On-Close persistence is the minimal fix: long-lived single-process daemons
// keep the same hot-path zero-cgo property (the runtime never touches disk),
// while CLI subcommands round-trip cleanly through the dump file. A process
// crash still loses everything — that matches the "volatile" documentation.

const (
	persistMagic   uint32 = 0x4d454d44 // "MEMD"
	persistVersion uint32 = 1
)

// saveToFile writes the entire DB state to `path` (overwriting any existing
// file). Safe to call after Close: callers serialise via the registryMu.
func (db *DB) saveToFile(path string) error {
	if path == "" {
		return nil
	}
	db.mu.RLock()
	tables := db.tables
	sequences := db.sequences
	db.mu.RUnlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("memstoredb: mkdir for save: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("memstoredb: create save file: %w", err)
	}
	w := bufio.NewWriterSize(f, 1<<20)

	write := func(err *error, vals ...any) {
		if *err != nil {
			return
		}
		for _, v := range vals {
			if e := binary.Write(w, binary.LittleEndian, v); e != nil {
				*err = e
				return
			}
		}
	}
	writeBytes := func(err *error, b []byte) {
		if *err != nil {
			return
		}
		if e := binary.Write(w, binary.LittleEndian, uint32(len(b))); e != nil {
			*err = e
			return
		}
		if _, e := w.Write(b); e != nil {
			*err = e
		}
	}

	var werr error
	write(&werr, persistMagic, persistVersion)
	write(&werr, uint32(len(sequences)))
	for name, val := range sequences {
		writeBytes(&werr, []byte(name))
		write(&werr, val)
	}
	write(&werr, uint32(len(tables)))
	for name, tab := range tables {
		writeBytes(&werr, []byte(name))
		var dup uint8
		if tab.dupSort {
			dup = 1
		}
		write(&werr, dup, uint64(tab.tree.Len()))
		iter := tab.tree.Iter()
		ok := iter.First()
		for ok && werr == nil {
			it := iter.Item()
			writeBytes(&werr, it.k)
			writeBytes(&werr, it.v)
			ok = iter.Next()
		}
		iter.Release()
		if werr != nil {
			break
		}
	}
	if werr != nil {
		_ = f.Close()
		_ = dir.RemoveFile(tmp)
		return fmt.Errorf("memstoredb: serialise: %w", werr)
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = dir.RemoveFile(tmp)
		return fmt.Errorf("memstoredb: flush save file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = dir.RemoveFile(tmp)
		return fmt.Errorf("memstoredb: close save file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("memstoredb: rename save file: %w", err)
	}
	return nil
}

// loadFromFile populates db from `path`. Returns nil if the file does not
// exist. The DB must be empty when called (constructor path only).
func (db *DB) loadFromFile(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("memstoredb: open save file: %w", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	read := func(err *error, dst any) {
		if *err != nil {
			return
		}
		if e := binary.Read(r, binary.LittleEndian, dst); e != nil {
			*err = e
		}
	}
	readBytes := func(err *error) []byte {
		if *err != nil {
			return nil
		}
		var n uint32
		if e := binary.Read(r, binary.LittleEndian, &n); e != nil {
			*err = e
			return nil
		}
		if n > 1<<30 {
			*err = fmt.Errorf("memstoredb: implausible byte length %d", n)
			return nil
		}
		b := make([]byte, n)
		if _, e := io.ReadFull(r, b); e != nil {
			*err = e
			return nil
		}
		return b
	}

	var rerr error
	var magic, ver uint32
	read(&rerr, &magic)
	read(&rerr, &ver)
	if rerr != nil {
		return fmt.Errorf("memstoredb: read header: %w", rerr)
	}
	if magic != persistMagic {
		return fmt.Errorf("memstoredb: bad magic %#x in %s", magic, path)
	}
	if ver != persistVersion {
		return fmt.Errorf("memstoredb: unsupported version %d in %s (want %d)", ver, path, persistVersion)
	}

	var nseq uint32
	read(&rerr, &nseq)
	for i := uint32(0); i < nseq && rerr == nil; i++ {
		name := readBytes(&rerr)
		var val uint64
		read(&rerr, &val)
		if rerr == nil {
			db.sequences[string(name)] = val
		}
	}

	var ntab uint32
	read(&rerr, &ntab)
	for i := uint32(0); i < ntab && rerr == nil; i++ {
		name := readBytes(&rerr)
		var dup uint8
		var nent uint64
		read(&rerr, &dup)
		read(&rerr, &nent)
		if rerr != nil {
			break
		}
		tab := newTable(dup != 0)
		for j := uint64(0); j < nent && rerr == nil; j++ {
			k := readBytes(&rerr)
			v := readBytes(&rerr)
			if rerr == nil {
				tab.tree.Set(entry{k: common.Copy(k), v: common.Copy(v)})
			}
		}
		if rerr == nil {
			db.tables[string(name)] = tab
		}
	}
	if rerr != nil {
		return fmt.Errorf("memstoredb: deserialise: %w", rerr)
	}
	return nil
}
