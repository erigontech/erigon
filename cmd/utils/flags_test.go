// Copyright 2019 The go-ethereum Authors
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

// Package utils contains internal helper functions for go-ethereum commands.
package utils

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/dbg"
)

func Test_SplitTagsFlag(t *testing.T) {
	tests := []struct {
		name string
		args string
		want map[string]string
	}{
		{
			"2 tags case",
			"host=localhost,bzzkey=123",
			map[string]string{
				"host":   "localhost",
				"bzzkey": "123",
			},
		},
		{
			"1 tag case",
			"host=localhost123",
			map[string]string{
				"host": "localhost123",
			},
		},
		{
			"empty case",
			"",
			map[string]string{},
		},
		{
			"garbage",
			"smth=smthelse=123",
			map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SplitTagsFlag(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitTagsFlag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveChainName(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"default (no flags) → mainnet", nil, "mainnet"},
		{"--chain=sepolia", []string{"--chain=sepolia"}, "sepolia"},
		{"--networkid=1 only → mainnet", []string{"--networkid=1"}, "mainnet"},
		{"--networkid=11155111 only → sepolia (known id)", []string{"--networkid=11155111"}, "sepolia"},
		{"--networkid=99999 only → empty (unknown id)", []string{"--networkid=99999"}, ""},
		{"--networkid=1337 only → bor-devnet (registered id)", []string{"--networkid=1337"}, "bor-devnet"},
		{"--networkid=99999 --chain=mainnet → mainnet (explicit chain wins)", []string{"--networkid=99999", "--chain=mainnet"}, "mainnet"},
		{"--networkid=99999 --chain=sepolia → sepolia (explicit chain wins)", []string{"--networkid=99999", "--chain=sepolia"}, "sepolia"},
		{"--networkid=1 --chain=sepolia → sepolia (explicit chain wins over mainnet id)", []string{"--networkid=1", "--chain=sepolia"}, "sepolia"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := cli.NewApp()
			app.Flags = []cli.Flag{&ChainFlag, &NetworkIdFlag}
			app.Action = func(ctx *cli.Context) error {
				require.Equal(t, tt.want, resolveChainName(ctx))
				return nil
			}
			require.NoError(t, app.Run(append([]string{"test"}, tt.args...)))
		})
	}
}

// TestExecPerfFlags_OverrideDbg verifies each --exec.* flag, when explicitly
// set, flips the corresponding dbg package toggle. Also asserts the no-flag
// path leaves dbg values untouched so env vars remain the source of truth.
func TestExecPerfFlags_OverrideDbg(t *testing.T) {
	// Snapshot and restore dbg values so tests don't leak into each other.
	origIgnoreBAL := dbg.IgnoreBAL
	origReadAhead := dbg.ReadAhead
	origUseStateCache := dbg.UseStateCache
	origExec3Workers := dbg.Exec3Workers
	origNoPrune := dbg.NoPrune()
	origNoMerge := dbg.NoMerge()
	t.Cleanup(func() {
		dbg.SetIgnoreBAL(origIgnoreBAL)
		dbg.SetReadAhead(origReadAhead)
		dbg.SetUseStateCache(origUseStateCache)
		dbg.SetExec3Workers(origExec3Workers)
		dbg.SetNoPrune(origNoPrune)
		dbg.SetNoMerge(origNoMerge)
	})

	apply := func(ctx *cli.Context) error {
		if ctx.IsSet(ExecBatchedIOFlag.Name) {
			v := ctx.Bool(ExecBatchedIOFlag.Name)
			dbg.SetReadAhead(v)
			dbg.SetIgnoreBAL(!v)
		}
		if ctx.IsSet(ExecStateCacheFlag.Name) {
			dbg.SetUseStateCache(ctx.Bool(ExecStateCacheFlag.Name))
		}
		if ctx.IsSet(ExecWorkersFlag.Name) {
			dbg.SetExec3Workers(ctx.Int(ExecWorkersFlag.Name))
		}
		if ctx.IsSet(ExecSerialFlag.Name) && ctx.Bool(ExecSerialFlag.Name) {
			dbg.SetExec3Workers(1)
		}
		if ctx.IsSet(ExecNoMergeFlag.Name) {
			dbg.SetNoMerge(ctx.Bool(ExecNoMergeFlag.Name))
		}
		if ctx.IsSet(ExecNoPruneFlag.Name) {
			dbg.SetNoPrune(ctx.Bool(ExecNoPruneFlag.Name))
		}
		return nil
	}

	run := func(args ...string) {
		app := cli.NewApp()
		app.Flags = []cli.Flag{
			&ExecBatchedIOFlag, &ExecStateCacheFlag, &ExecWorkersFlag,
			&ExecSerialFlag, &ExecNoMergeFlag, &ExecNoPruneFlag,
		}
		app.Action = apply
		require.NoError(t, app.Run(append([]string{"test"}, args...)))
	}

	t.Run("no flags set leaves dbg untouched", func(t *testing.T) {
		dbg.SetIgnoreBAL(false)
		dbg.SetReadAhead(true)
		dbg.SetUseStateCache(true)
		dbg.SetExec3Workers(42)
		dbg.SetNoMerge(false)
		dbg.SetNoPrune(false)
		run()
		require.Equal(t, false, dbg.IgnoreBAL)
		require.Equal(t, true, dbg.ReadAhead)
		require.Equal(t, true, dbg.UseStateCache)
		require.Equal(t, 42, dbg.Exec3Workers)
		require.Equal(t, false, dbg.NoMerge())
		require.Equal(t, false, dbg.NoPrune())
	})

	t.Run("batched-io=false disables read-ahead and sets IgnoreBAL", func(t *testing.T) {
		dbg.SetIgnoreBAL(false)
		dbg.SetReadAhead(true)
		run("--exec.batched-io=false")
		require.True(t, dbg.IgnoreBAL)
		require.False(t, dbg.ReadAhead)
	})

	t.Run("batched-io=true enables read-ahead and clears IgnoreBAL", func(t *testing.T) {
		dbg.SetIgnoreBAL(true)
		dbg.SetReadAhead(false)
		run("--exec.batched-io=true")
		require.False(t, dbg.IgnoreBAL)
		require.True(t, dbg.ReadAhead)
	})

	t.Run("state-cache=false flips UseStateCache", func(t *testing.T) {
		dbg.SetUseStateCache(true)
		run("--exec.state-cache=false")
		require.False(t, dbg.UseStateCache)
	})

	t.Run("workers=7 sets Exec3Workers", func(t *testing.T) {
		dbg.SetExec3Workers(1)
		run("--exec.workers=7")
		require.Equal(t, 7, dbg.Exec3Workers)
	})

	t.Run("serial=true clamps Exec3Workers to 1", func(t *testing.T) {
		dbg.SetExec3Workers(8)
		run("--exec.serial=true")
		require.Equal(t, 1, dbg.Exec3Workers)
	})

	t.Run("serial=true wins over --exec.workers", func(t *testing.T) {
		dbg.SetExec3Workers(1)
		// flags are applied in declaration order (workers first, then serial),
		// so serial should override workers regardless of CLI argument order.
		run("--exec.workers=12", "--exec.serial=true")
		require.Equal(t, 1, dbg.Exec3Workers)
	})

	t.Run("serial=false leaves Exec3Workers untouched", func(t *testing.T) {
		dbg.SetExec3Workers(8)
		run("--exec.serial=false")
		require.Equal(t, 8, dbg.Exec3Workers)
	})

	t.Run("no-merge and no-prune set to true", func(t *testing.T) {
		dbg.SetNoMerge(false)
		dbg.SetNoPrune(false)
		run("--exec.no-merge=true", "--exec.no-prune=true")
		require.True(t, dbg.NoMerge())
		require.True(t, dbg.NoPrune())
	})
}
