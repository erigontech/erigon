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

package diskutils

import (
	"bufio"
	"fmt"
	"io"
	"slices"
	"strings"
)

// fsTypeFromMountinfo returns the filesystem type of the mount whose major:minor
// device ID is devID. Format: mountID parentID major:minor root mountPoint
// options [optional fields] - fsType source superOptions
//
// Uses bufio.Reader instead of bufio.Scanner: an overlay2 mount's lowerdir=
// superoption can list many image layers on one line, and Scanner aborts the
// whole scan once any single line exceeds its buffer cap.
func fsTypeFromMountinfo(mountinfo io.Reader, devID string) (string, error) {
	reader := bufio.NewReaderSize(mountinfo, 64*1024)
	for {
		line, readErr := reader.ReadString('\n')
		if fsType, ok := fsTypeFromMountinfoLine(line, devID); ok {
			return fsType, nil
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return "", readErr
		}
	}
	return "", fmt.Errorf("no mountinfo entry for device %s", devID)
}

func fsTypeFromMountinfoLine(line, devID string) (string, bool) {
	fields := strings.Fields(line)
	separator := slices.Index(fields, "-")
	if separator < 6 || separator+1 >= len(fields) || fields[2] != devID {
		return "", false
	}
	return fields[separator+1], true
}
