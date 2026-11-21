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

package flags

import "github.com/spf13/pflag"

// dirValue is a pflag.Value that expands a leading "~/" (or "~\" on Windows) and
// "$VAR" in the assigned path, mirroring the urfave DirectoryFlag used by the
// main erigon binary.
type dirValue struct{ p *string }

func (d *dirValue) String() string {
	if d.p == nil {
		return ""
	}
	return *d.p
}

func (d *dirValue) Set(s string) error {
	if s != "" {
		s = expandPath(s)
	}
	*d.p = s
	return nil
}

func (d *dirValue) Type() string { return "string" }

// DirVar registers a cobra/pflag string flag for a directory path, expanding a
// leading "~/" (or "~\" on Windows) and "$VAR" at parse time so all binaries
// normalize --datadir identically to the erigon binary's DirectoryFlag.
func DirVar(fs *pflag.FlagSet, p *string, name, value, usage string) {
	if value != "" {
		value = expandPath(value)
	}
	*p = value
	fs.Var(&dirValue{p}, name, usage)
}
