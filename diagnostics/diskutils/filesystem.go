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
	"slices"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
)

const filesystemDocsURL = "https://docs.erigon.tech/get-started/hardware-requirements#filesystems-to-avoid"

var recommendedFilesystems = []string{"ext4", "xfs"}

func IsRecommendedFilesystem(fsType string) bool {
	return slices.Contains(recommendedFilesystems, strings.ToLower(fsType))
}

// WarnUnlessRecommendedFilesystem logs a warning when dirPath does not sit on
// ext4 or XFS, the only filesystems Erigon measures performance on.
func WarnUnlessRecommendedFilesystem(logger log.Logger, dirPath string) {
	fsType, err := FilesystemType(dirPath)
	if err != nil {
		logger.Debug("[diskutils] Cannot detect filesystem type", "path", dirPath, "err", err)
		return
	}
	if IsRecommendedFilesystem(fsType) {
		return
	}
	logger.Warn("[diskutils] Filesystem is neither ext4 nor XFS, performance is unverified",
		"path", dirPath, "fstype", fsType, "see", filesystemDocsURL)
}
