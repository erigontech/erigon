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

package bscsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runSvcUntilCancelled(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func runWithServiceAsync(ctx context.Context, svc, download func(context.Context) error) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- runWithService(ctx, svc, download) }()
	return errCh
}

func requireReturns(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("runWithService did not return")
		return nil
	}
}

func TestRunWithService_DownloadDoneStopsService(t *testing.T) {
	t.Parallel()
	svcStopped := make(chan struct{})
	svc := func(ctx context.Context) error {
		defer close(svcStopped)
		return runSvcUntilCancelled(ctx)
	}
	download := func(context.Context) error { return nil }

	err := requireReturns(t, runWithServiceAsync(t.Context(), svc, download))
	require.NoError(t, err)
	select {
	case <-svcStopped:
	default:
		t.Fatal("service still running after runWithService returned")
	}
}

func TestRunWithService_ServiceFailureStopsDownload(t *testing.T) {
	t.Parallel()
	svcErr := errors.New("service failed")
	svc := func(context.Context) error { return svcErr }

	err := requireReturns(t, runWithServiceAsync(t.Context(), svc, runSvcUntilCancelled))
	require.ErrorIs(t, err, svcErr)
}

func TestRunWithService_ParentCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	errCh := runWithServiceAsync(ctx, runSvcUntilCancelled, runSvcUntilCancelled)
	cancel()

	err := requireReturns(t, errCh)
	require.ErrorIs(t, err, context.Canceled)
}
