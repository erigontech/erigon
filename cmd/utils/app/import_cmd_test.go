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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestImportFilesProcessesEveryFile(t *testing.T) {
	files := []string{"0001.rlp", "0002.rlp", "0003.rlp"}
	var imported []string
	err := importFiles(files, log.Root(), func(fn string) error {
		imported = append(imported, fn)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, files, imported)
}

func TestImportFilesContinuesPastPerFileFailure(t *testing.T) {
	files := []string{"0001.rlp", "0002.rlp", "0003.rlp"}
	badBlock := errors.New("invalid block")
	var imported []string
	err := importFiles(files, log.Root(), func(fn string) error {
		imported = append(imported, fn)
		if fn == "0002.rlp" {
			return badBlock
		}
		return nil
	})
	require.ErrorIs(t, err, badBlock)
	require.Equal(t, files, imported, "a failing block file must not stop import of later files")
}

func TestImportFilesSingleFileSurfacesError(t *testing.T) {
	badBlock := errors.New("invalid block")
	err := importFiles([]string{"0001.rlp"}, log.Root(), func(fn string) error {
		return badBlock
	})
	require.ErrorIs(t, err, badBlock)
}
