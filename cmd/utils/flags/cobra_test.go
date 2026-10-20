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

import (
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
)

func TestDirVarExpandsTilde(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var got string
	DirVar(fs, &got, "datadir", "", "")
	if err := fs.Parse([]string{"--datadir=~/erigon"}); err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(home, "erigon")
	if got != want {
		t.Fatalf("bound var not expanded: got %q want %q", got, want)
	}
	if v := fs.Lookup("datadir").Value.String(); v != want {
		t.Fatalf("flag lookup not expanded: got %q want %q", v, want)
	}
}

func TestDirVarExpandsEnv(t *testing.T) {
	home := t.TempDir()
	t.Setenv("MYDATA", filepath.Join(home, "d"))

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var got string
	DirVar(fs, &got, "datadir", "", "")
	if err := fs.Parse([]string{"--datadir=$MYDATA/chain"}); err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(home, "d", "chain")
	if got != want {
		t.Fatalf("env var not expanded: got %q want %q", got, want)
	}
	if v := fs.Lookup("datadir").Value.String(); v != want {
		t.Fatalf("flag lookup not expanded: got %q want %q", v, want)
	}
}

func TestDirVarEmptyDefaultStaysEmpty(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var got string
	DirVar(fs, &got, "datadir", "", "")
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("empty default must stay empty, got %q", got)
	}
}
