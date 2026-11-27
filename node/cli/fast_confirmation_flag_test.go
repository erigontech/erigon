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

package cli

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/cmd/caplin/caplinflags"
	"github.com/stretchr/testify/require"
	urfave "github.com/urfave/cli/v3"
)

func TestFastConfirmationFlag(t *testing.T) {
	for name, flags := range map[string][]urfave.Flag{"erigon": DefaultFlags, "caplin": caplinflags.CliFlags} {
		t.Run(name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				var selected []urfave.Flag
				for _, flag := range flags {
					if flag.Names()[0] == "enable-fast-confirmation" {
						cloned := *flag.(*urfave.BoolFlag)
						selected = append(selected, &cloned)
					}
				}
				cmd := &urfave.Command{Flags: selected, Action: func(_ context.Context, cmd *urfave.Command) error {
					require.Equal(t, enabled, cmd.Bool("enable-fast-confirmation"))
					return nil
				}}
				args := []string{name}
				if enabled {
					args = append(args, "--enable-fast-confirmation")
				}
				require.NoError(t, cmd.Run(t.Context(), args))
			}
		})
	}
}
