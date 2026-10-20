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
package deriver

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common"
	commondir "github.com/erigontech/erigon/common/dir"
)

// CheckpointFile is the checkpoint's name in its directory, the reference
// client's.
const CheckpointFile = "eez-reconcile-checkpoint"

const checkpointVersion = "eez-reconcile-checkpoint v2"

// ErrCheckpoint reports a checkpoint that cannot seed a resume.
var ErrCheckpoint = errors.New("checkpoint unusable")

// Checkpoint is the last settlement indexed before a stop, in the reference
// client's file format.
type Checkpoint struct {
	L1Block     uint64
	L1BlockHash common.Hash
	TxHash      common.Hash
	L2Cursor    uint64
	L2BlockHash common.Hash
}

func (c *Checkpoint) encode() string {
	return fmt.Sprintf("%s\nl1_block=%d\nl1_block_hash=%s\ntx_hash=%s\nl2_cursor=%d\nl2_block_hash=%s\n",
		checkpointVersion, c.L1Block, c.L1BlockHash.Hex(), c.TxHash.Hex(), c.L2Cursor, c.L2BlockHash.Hex())
}

// Save writes the checkpoint to dir atomically.
func (c *Checkpoint) Save(dir string) error {
	path := filepath.Join(dir, CheckpointFile)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(c.encode()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// RemoveCheckpoint removes the checkpoint in dir, if any.
func RemoveCheckpoint(dir string) error {
	err := commondir.RemoveFile(filepath.Join(dir, CheckpointFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// LoadCheckpoint reads the checkpoint in dir; it returns nil without one.
func LoadCheckpoint(dir string) (*Checkpoint, error) {
	data, err := os.ReadFile(filepath.Join(dir, CheckpointFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeCheckpoint(string(data))
}

func decodeCheckpoint(raw string) (*Checkpoint, error) {
	s := bufio.NewScanner(strings.NewReader(raw))
	if !s.Scan() || strings.TrimSpace(s.Text()) != checkpointVersion {
		return nil, fmt.Errorf("%w: not a %q file", ErrCheckpoint, checkpointVersion)
	}
	fields := map[string]string{}
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%w: line %q", ErrCheckpoint, line)
		}
		switch key {
		case "l1_block", "l1_block_hash", "tx_hash", "l2_cursor", "l2_block_hash":
		default:
			return nil, fmt.Errorf("%w: unknown key %q", ErrCheckpoint, key)
		}
		fields[key] = value
	}
	if len(fields) != 5 {
		return nil, fmt.Errorf("%w: %d of 5 fields", ErrCheckpoint, len(fields))
	}
	var c Checkpoint
	var err error
	if c.L1Block, err = strconv.ParseUint(fields["l1_block"], 10, 64); err != nil {
		return nil, fmt.Errorf("%w: l1_block: %w", ErrCheckpoint, err)
	}
	if c.L2Cursor, err = strconv.ParseUint(fields["l2_cursor"], 10, 64); err != nil {
		return nil, fmt.Errorf("%w: l2_cursor: %w", ErrCheckpoint, err)
	}
	for key, dst := range map[string]*common.Hash{"l1_block_hash": &c.L1BlockHash, "tx_hash": &c.TxHash, "l2_block_hash": &c.L2BlockHash} {
		v := fields[key]
		if len(v) != 66 || !strings.HasPrefix(v, "0x") {
			return nil, fmt.Errorf("%w: %s %q", ErrCheckpoint, key, v)
		}
		if err := dst.UnmarshalText([]byte(v)); err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrCheckpoint, key, err)
		}
	}
	return &c, nil
}

// UsableWith accepts the checkpoint when its L1 block is still canonical
// and the local L2 block at its cursor is the one it names; a nil hash is a
// block the source does not serve.
func (c *Checkpoint) UsableWith(canonicalL1, localL2 *common.Hash) error {
	switch {
	case canonicalL1 == nil:
		return fmt.Errorf("%w: L1 does not serve block %d", ErrCheckpoint, c.L1Block)
	case *canonicalL1 != c.L1BlockHash:
		return fmt.Errorf("%w: L1 block %d was reorged out", ErrCheckpoint, c.L1Block)
	case localL2 == nil:
		return fmt.Errorf("%w: no local L2 block %d", ErrCheckpoint, c.L2Cursor)
	case *localL2 != c.L2BlockHash:
		return fmt.Errorf("%w: local L2 block %d differs", ErrCheckpoint, c.L2Cursor)
	}
	return nil
}
