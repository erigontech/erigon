// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

import (
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/math"
)

// DirectoryString is custom type which is registered in the flags library which cli uses for
// argument parsing. This allows us to expand Value to an absolute path when
// the argument is parsed
type DirectoryString string

func (s *DirectoryString) String() string {
	return string(*s)
}

func (s *DirectoryString) Set(value string) error {
	*s = DirectoryString(expandPath(value))
	return nil
}

func (s *DirectoryString) Get() any {
	return s.String()
}

// DirectoryFlag is a cli.Flag type which expands the received string to an absolute path.
// e.g. ~/.ethereum -> /home/username/.ethereum
type DirectoryFlag = cli.FlagBase[string, cli.StringConfig, directoryValue]

// directoryValue is the cli.ValueCreator backing DirectoryFlag; it expands the
// path when the flag is set.
type directoryValue struct {
	destination *string
}

func (directoryValue) Create(val string, p *string, c cli.StringConfig) cli.Value {
	*p = val
	return &directoryValue{destination: p}
}

func (directoryValue) ToString(val string) string {
	if val == "" {
		return ""
	}
	return fmt.Sprintf("%q", val)
}

func (d *directoryValue) Set(val string) error {
	*d.destination = expandPath(val)
	return nil
}

func (d *directoryValue) Get() any { return *d.destination }

func (d *directoryValue) String() string {
	if d.destination != nil && *d.destination != "" {
		return fmt.Sprintf("%q", *d.destination)
	}
	return ""
}

// BigFlag is a command line flag that accepts 256 bit big integers in decimal or
// hexadecimal syntax.
type BigFlag = cli.FlagBase[*big.Int, cli.NoConfig, bigValue]

// bigValue is the cli.ValueCreator backing BigFlag.
type bigValue struct {
	destination **big.Int
}

func (bigValue) Create(val *big.Int, p **big.Int, c cli.NoConfig) cli.Value {
	*p = val
	return &bigValue{destination: p}
}

func (bigValue) ToString(val *big.Int) string {
	if val == nil {
		return ""
	}
	return val.String()
}

func (b *bigValue) Set(s string) error {
	intVal, ok := math.ParseBig256(s)
	if !ok {
		return errors.New("invalid integer syntax")
	}
	*b.destination = intVal
	return nil
}

func (b *bigValue) Get() any { return *b.destination }

func (b *bigValue) String() string {
	if b.destination == nil || *b.destination == nil {
		return ""
	}
	return (*b.destination).String()
}

// GlobalBig returns the value of a BigFlag from the global flag set. It never
// returns nil so callers can dereference the result unconditionally.
func GlobalBig(cmd *cli.Command, name string) *big.Int {
	if v, ok := cmd.Value(name).(*big.Int); ok && v != nil {
		return v
	}
	return new(big.Int)
}

// Expands a file path
// 1. replace tilde with users home dir
// 2. expands embedded environment variables
// 3. cleans the path, e.g. /a/b/../c -> /a/c
// Note, it has limitations, e.g. ~someuser/tmp will not be expanded
func expandPath(p string) string {
	// Named pipes are not file paths on windows, ignore
	if strings.HasPrefix(p, `\\.\pipe`) {
		return p
	}
	if strings.HasPrefix(p, "~/") || strings.HasPrefix(p, "~\\") {
		if home := HomeDir(); home != "" {
			p = home + p[1:]
		}
	}
	return filepath.Clean(os.ExpandEnv(p))
}

func HomeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	if usr, err := user.Current(); err == nil {
		return usr.HomeDir
	}
	return ""
}

func DBPageSizeFlagUnmarshal(cliCtx *cli.Command, flagName, flagUsage string) datasize.ByteSize {
	var pageSize datasize.ByteSize
	if err := pageSize.UnmarshalText([]byte(cliCtx.String(flagName))); err != nil {
		panic(err)
	}
	sz := pageSize.Bytes()
	if !isPowerOfTwo(sz) || sz < 256 || sz > 64*1024 {
		panic(fmt.Errorf("invalid --%s: %d, see: %s", flagName, sz, flagUsage))
	}
	return pageSize
}

func isPowerOfTwo(n uint64) bool {
	if n == 0 { //corner case: if n is zero it will also consider as power 2
		return true
	}
	return n&(n-1) == 0
}
