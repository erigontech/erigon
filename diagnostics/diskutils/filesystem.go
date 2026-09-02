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
	"path/filepath"
	"slices"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
)

const filesystemDocsURL = "https://docs.erigon.tech/get-started/hardware-requirements"

var recommendedFilesystems = []string{"ext4", "xfs"}

func IsRecommendedFilesystem(fsType string) bool {
	return slices.Contains(recommendedFilesystems, strings.ToLower(fsType))
}

// CheckFilesystem logs a warning for each filesystem behind dirPaths that is
// neither ext4 nor XFS, the only ones Erigon measures performance on. Erigon
// dirs can live on separate mounts, so they are reported per filesystem type.
func CheckFilesystem(logger log.Logger, dirPaths ...string) {
	groups, failed := unrecommendedByType(dirPaths, FilesystemType)
	for dirPath, err := range failed {
		logger.Debug("[diskutils] Cannot detect filesystem type", "path", dirPath, "err", err)
	}
	for _, group := range groups {
		logger.Warn("[diskutils] Filesystem is neither ext4 nor XFS, performance is unverified",
			"fstype", group.fsType, "paths", strings.Join(group.paths, ", "), "see", filesystemDocsURL)
	}
}

type fsGroup struct {
	fsType string
	paths  []string
}

func unrecommendedByType(dirPaths []string, fsTypeOf func(string) (string, error)) ([]fsGroup, map[string]error) {
	var groups []fsGroup
	failed := map[string]error{}
	seen := map[string]bool{}

	for _, dirPath := range dirPaths {
		if dirPath == "" || seen[dirPath] {
			continue
		}
		seen[dirPath] = true

		fsType, err := fsTypeOf(dirPath)
		if err != nil {
			failed[dirPath] = err
			continue
		}
		if IsRecommendedFilesystem(fsType) {
			continue
		}
		if i := slices.IndexFunc(groups, func(g fsGroup) bool { return strings.EqualFold(g.fsType, fsType) }); i >= 0 {
			groups[i].add(dirPath)
			continue
		}
		groups = append(groups, fsGroup{fsType: fsType, paths: []string{dirPath}})
	}
	return groups, failed
}

// add keeps only the topmost dirs, so dirs sharing one mount are reported once.
func (g *fsGroup) add(dirPath string) {
	if slices.ContainsFunc(g.paths, func(p string) bool { return isAncestor(p, dirPath) }) {
		return
	}
	g.paths = slices.DeleteFunc(g.paths, func(p string) bool { return isAncestor(dirPath, p) })
	g.paths = append(g.paths, dirPath)
}

func isAncestor(ancestor, descendant string) bool {
	rel, err := filepath.Rel(ancestor, descendant)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
