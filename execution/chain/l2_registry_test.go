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
package chain

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type namedL2 struct {
	Name_ string `json:"name"`
	Value int    `json:"value"`
}

func (c *namedL2) Name() string                                                 { return c.Name_ }
func (c *namedL2) ResolveRules(l2Version, blockNum, blockTime uint64, r *Rules) {}

func registerTestL2(t *testing.T, name string) {
	t.Helper()
	RegisterL2Config(name, func(raw json.RawMessage) (L2Config, error) {
		var c namedL2
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		if c.Value < 0 {
			return nil, errors.New("negative value")
		}
		return &c, nil
	})
	t.Cleanup(func() { unregisterL2Config(name) })
}

func TestResolveL2(t *testing.T) {
	registerTestL2(t, "resolvel2")

	var c Config
	require.NoError(t, json.Unmarshal([]byte(`{"chainId":1,"l2":{"name":"resolvel2","value":7}}`), &c))
	require.Nil(t, c.L2)
	require.NoError(t, c.ResolveL2())
	require.Equal(t, &namedL2{Name_: "resolvel2", Value: 7}, c.L2)

	resolved := c.L2
	c.L2JSON = json.RawMessage(`{"name":"resolvel2","value":8}`)
	require.NoError(t, c.ResolveL2())
	require.Same(t, resolved, c.L2)
}

func TestResolveL2WithoutSection(t *testing.T) {
	for _, raw := range []string{`{"chainId":1}`, `{"chainId":1,"l2":null}`} {
		var c Config
		require.NoError(t, json.Unmarshal([]byte(raw), &c))
		require.NoError(t, c.ResolveL2())
		require.Nil(t, c.L2)
	}
	var nilCfg *Config
	require.NoError(t, nilCfg.ResolveL2())
}

func TestResolveL2Errors(t *testing.T) {
	registerTestL2(t, "resolvel2")

	for _, tc := range []struct {
		name, l2 string
		unknown  bool
	}{
		{"unregistered name", `{"name":"other"}`, true},
		{"no name", `{"value":1}`, true},
		{"not an object", `[1]`, false},
		{"decoder error", `{"name":"resolvel2","value":-1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{L2JSON: json.RawMessage(tc.l2)}
			err := c.ResolveL2()
			require.Error(t, err)
			require.Equal(t, tc.unknown, errors.Is(err, ErrUnknownL2))
			require.Nil(t, c.L2)
		})
	}
}

func TestRegisterL2ConfigPanics(t *testing.T) {
	registerTestL2(t, "dupl2")
	decode := func(json.RawMessage) (L2Config, error) { return nil, nil }
	require.Panics(t, func() { RegisterL2Config("dupl2", decode) })
	require.Panics(t, func() { RegisterL2Config("", decode) })
	require.Panics(t, func() { RegisterL2Config("nildecoder", nil) })
}
