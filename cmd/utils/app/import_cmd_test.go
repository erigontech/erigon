// Copyright 2024 The Erigon Authors
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func quietLogger() log.Logger {
	l := log.New()
	l.SetHandler(log.DiscardHandler())
	return l
}

func TestImportFilesImportsEveryFileInOrder(t *testing.T) {
	files := []string{"0001.rlp", "0002.rlp", "0003.rlp"}
	var seen []string
	err := importFiles(files, func(fn string) error {
		seen = append(seen, fn)
		return nil
	}, quietLogger())
	require.NoError(t, err)
	assert.Equal(t, files, seen)
}

func TestImportFilesSingleFileErrorIsFatal(t *testing.T) {
	wantErr := errors.New("bad block")
	err := importFiles([]string{"only.rlp"}, func(string) error {
		return wantErr
	}, quietLogger())
	require.ErrorIs(t, err, wantErr)
}

func TestImportFilesMultiFileToleratesPerFileError(t *testing.T) {
	files := []string{"0001.rlp", "bad.rlp", "0003.rlp"}
	var seen []string
	err := importFiles(files, func(fn string) error {
		seen = append(seen, fn)
		if fn == "bad.rlp" {
			return errors.New("bad block")
		}
		return nil
	}, quietLogger())
	require.NoError(t, err)
	assert.Equal(t, files, seen)
}
