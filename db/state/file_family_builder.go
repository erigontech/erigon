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

package state

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

// BuildDomainFamily runs collate + buildFiles for one step of a domain
// and wraps the resulting StaticFiles into a DomainFileFamily. The three
// members (.kv, .ef, .v) share the same [txFrom, txTo) range by
// construction; NewDomainFileFamily enforces that invariant. On any
// error along the way, every already-opened file is closed and removed.
//
// Returns (nil, nil) when the domain is disabled or the build produced
// no output — matching the existing integrateDirtyFiles skip-on-nil
// semantic. Callers must treat that as "no family for this step",
// exactly as they treat a StaticFiles with a nil valuesDecomp today.
func BuildDomainFamily(ctx context.Context, d *Domain, step kv.Step, txFrom, txTo uint64, tx kv.Tx, ps *background.ProgressSet) (*DomainFileFamily, error) {
	dom, err := kv.String2Domain(d.FilenameBase)
	if err != nil {
		return nil, err
	}

	coll, err := d.collate(ctx, step, txFrom, txTo, tx)
	if err != nil {
		return nil, err
	}

	sf, err := d.buildFiles(ctx, step, coll, ps)
	coll.Close()
	if err != nil {
		sf.CleanupOnError()
		return nil, err
	}

	family, err := staticFilesToFamily(dom, sf, txFrom, txTo)
	if err != nil {
		sf.CleanupOnError()
		return nil, err
	}
	return family, nil
}

// staticFilesToFamily maps the flat StaticFiles struct produced by
// Domain.buildFiles into three FilesItems wrapped in a
// DomainFileFamily. Returns (nil, nil) when the build was skipped
// (nil valuesDecomp) — same skip semantic as integrateDirtyFiles.
func staticFilesToFamily(dom kv.Domain, sf StaticFiles, txFrom, txTo uint64) (*DomainFileFamily, error) {
	if sf.valuesDecomp == nil {
		return nil, nil
	}
	if sf.efHistoryDecomp == nil {
		return nil, fmt.Errorf("%w: efHistoryDecomp (.ef)", ErrFileFamilyMissingMember)
	}
	if sf.historyDecomp == nil {
		return nil, fmt.Errorf("%w: historyDecomp (.v)", ErrFileFamilyMissingMember)
	}

	values := newFilesItem(txFrom, txTo)
	values.version, _ = version.ParseVersion(filepath.Base(sf.valuesDecomp.FilePath()))
	values.decompressor = sf.valuesDecomp
	values.index = sf.valuesIdx
	values.bindex = sf.valuesBt
	values.existence = sf.existenceFilter

	index := newFilesItem(txFrom, txTo)
	index.decompressor = sf.efHistoryDecomp
	index.index = sf.efHistoryIdx
	index.existence = sf.efExistence

	history := newFilesItem(txFrom, txTo)
	history.decompressor = sf.historyDecomp
	history.index = sf.historyIdx

	return NewDomainFileFamily(dom, values, index, history)
}
