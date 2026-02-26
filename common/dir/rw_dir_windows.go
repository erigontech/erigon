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

//go:build windows

package dir

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func ReadDir(name string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(name)
	if err != nil {
		// some windows remote drived return this error
		// when they are empty - should really be handled
		// in os.ReadDir but is not
		// - looks likely fixed in go 1.22
		if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
			return nil, nil
		}
	}
	return files, err
}
