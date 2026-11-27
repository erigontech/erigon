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
	"errors"
	"fmt"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func commandReturning(err error) *cobra.Command {
	return &cobra.Command{
		Use:  "test",
		RunE: func(*cobra.Command, []string) error { return err },
	}
}

func TestRunIgnoresCancelOnlyAfterStop(t *testing.T) {
	canceled := fmt.Errorf("apply migrations: %w", context.Canceled)

	stopped, stop := context.WithCancel(t.Context())
	stop()
	require.NoError(t, run(stopped, commandReturning(canceled)))

	require.ErrorIs(t, run(t.Context(), commandReturning(canceled)), context.Canceled)
	require.EqualError(t, run(stopped, commandReturning(errors.New("boom"))), "boom")
	require.NoError(t, run(t.Context(), commandReturning(nil)))
}
