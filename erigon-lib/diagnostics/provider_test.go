// Copyright 2024 The Erigon Authors
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

package diagnostics_test

import (
	"context"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/log/v3"
)

type testInfo struct {
	count int
}

func (ti testInfo) Type() diagnostics.Type {
	return diagnostics.TypeOf(ti)
}

func StartDiagnostics(ctx context.Context) error {
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()

	var count int

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			diagnostics.Send(testInfo{count})
			count++
		}
	}
}

func TestProviderRegistration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// diagnostics receiver
	ctx, ch, cancel := diagnostics.Context[testInfo](context.Background(), 1)
	diagnostics.StartProviders(ctx, diagnostics.TypeOf(testInfo{}), log.Root())

	go StartDiagnostics(ctx)

	for info := range ch {
		if info.count == 3 {
			cancel()
		}
	}
}
