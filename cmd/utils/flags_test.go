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
