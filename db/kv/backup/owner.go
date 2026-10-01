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

//go:build !windows

package backup

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/erigontech/erigon/db/kv"
)

func closeBeforeRename(kv.RoDB) {}

// restoreOwner gives path the uid/gid src was stat'ed with, so a compaction run
// as root doesn't leave behind a data file the node's own user can't open.
func restoreOwner(src os.FileInfo, path string) error {
	st, ok := src.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	dst, err := os.Stat(path)
	if err != nil {
		return err
	}
	owner, ok := dst.Sys().(*syscall.Stat_t)
	if ok && owner.Uid == st.Uid && owner.Gid == st.Gid {
		return nil
	}
	err = os.Chown(path, int(st.Uid), int(st.Gid))
	if err != nil && ok {
		if errors.Is(err, os.ErrPermission) {
			err = fmt.Errorf("%w; if running in docker, use --user uid:gid matching the database ownership", err)
		}
		return fmt.Errorf("cannot preserve ownership (database=%d:%d, copy=%d:%d): %w", st.Uid, st.Gid, owner.Uid, owner.Gid, err)
	}
	return err
}
