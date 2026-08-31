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

package main

import (
	"fmt"
	"path/filepath"
	"regexp"
)

type testFilter struct {
	run      *regexp.Regexp
	excludes []*regexp.Regexp
}

func compileTestFilter(run string, excludes []string) (testFilter, error) {
	runPattern, err := regexp.Compile(run)
	if err != nil {
		return testFilter{}, fmt.Errorf("invalid regex --run: %w", err)
	}
	filter := testFilter{
		run:      runPattern,
		excludes: make([]*regexp.Regexp, 0, len(excludes)),
	}
	for _, pattern := range excludes {
		exclude, err := regexp.Compile(pattern)
		if err != nil {
			return testFilter{}, fmt.Errorf("invalid regex --exclude %q: %w", pattern, err)
		}
		filter.excludes = append(filter.excludes, exclude)
	}
	return filter, nil
}

func (f testFilter) includeFile(path string) bool {
	path = filepath.ToSlash(path)
	for _, exclude := range f.excludes {
		if exclude.MatchString(path) {
			return false
		}
	}
	return true
}

func (f testFilter) includeCase(path, name string) bool {
	if !f.run.MatchString(name) || !f.includeFile(path) {
		return false
	}
	id := filepath.ToSlash(path) + "::" + name
	for _, exclude := range f.excludes {
		if exclude.MatchString(id) {
			return false
		}
	}
	return true
}

func (f testFilter) filterFiles(files []string) []string {
	filtered := make([]string, 0, len(files))
	for _, file := range files {
		if f.includeFile(file) {
			filtered = append(filtered, file)
		}
	}
	return filtered
}
