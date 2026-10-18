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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/eez"
)

func TestRegisterEEZ(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{[]string{"erigon"}, false},
		{[]string{"erigon", "--eez.enabled"}, true},
		{[]string{"erigon", "sub", "leaf"}, false},
		{[]string{"erigon", "--eez.enabled", "sub", "leaf"}, true},
		{[]string{"erigon", "sub", "--eez.enabled", "leaf"}, true},
		{[]string{"erigon", "sub", "leaf", "--eez.enabled"}, true},
	} {
		var before, registered, ran bool
		action := func(context.Context, *cli.Command) error {
			ran = true
			// Registration must precede every action, the root's included.
			require.Equal(t, tc.want, registered, "args %q", tc.args)
			return nil
		}
		app := &cli.Command{
			Name:  "erigon",
			Flags: eez.Flags,
			Before: func(ctx context.Context, _ *cli.Command) (context.Context, error) {
				before = true
				return ctx, nil
			},
			Action: action,
			Commands: []*cli.Command{{
				Name:     "sub",
				Commands: []*cli.Command{{Name: "leaf", Action: action}},
			}},
		}
		registerEEZ(app, func() {
			require.True(t, before, "args %q", tc.args)
			registered = true
		})
		require.NoError(t, app.Run(context.Background(), tc.args))
		require.True(t, ran, "args %q", tc.args)
	}
}
