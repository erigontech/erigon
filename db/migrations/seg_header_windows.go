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

package migrations

import (
	"os"

	"golang.org/x/sys/windows"
)

// replacesSharedFiles: all links of a file share its read-only flag, so replacing a
// read-only hardlinked file changes the flag of the other links too.
const replacesSharedFiles = false

func sharesInode(path string, _ os.FileInfo) bool {
	f, err := os.Open(path)
	if err != nil {
		return true // a private copy is always safe
	}
	defer f.Close()
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &info); err != nil {
		return true
	}
	return info.NumberOfLinks > 1
}
