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
func fsTypeFromMountinfo(mountinfo io.Reader, devID string) (string, error) {
	scanner := bufio.NewScanner(mountinfo)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := slices.Index(fields, "-")
		if separator < 3 || separator+1 >= len(fields) {
			continue
		}
		if fields[2] == devID {
			return fields[separator+1], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no mountinfo entry for device %s", devID)
}
