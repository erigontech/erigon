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

package httpreqresp

import (
	"context"
	"net/http"
	"runtime"
	"testing"
	"time"
)

// TestDoContextCancel verifies that cancelling the context doesn't cause goroutine leaks
func TestDoContextCancel(t *testing.T) {
	// Count initial goroutines
	initialGoroutines := runtime.NumGoroutine()

	// Create a slow handler that takes longer than our context timeout
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // Sleep longer than context timeout
		w.WriteHeader(http.StatusOK)
	})

	// Create context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Create request with the context
	req, err := http.NewRequestWithContext(ctx, "GET", "/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Call Do - this should timeout due to context cancellation
	_, err = Do(handler, req)
	if err == nil {
		t.Fatal("Expected context cancellation error")
	}

	// Wait a bit for any potential goroutine cleanup
	time.Sleep(200 * time.Millisecond)

	// Force GC to clean up any terminated goroutines
	runtime.GC()
	runtime.GC()

	// Check goroutine count - should be back to initial or very close
	finalGoroutines := runtime.NumGoroutine()

	// Allow for some variance in goroutine count (test runner, GC, etc.)
	if finalGoroutines > initialGoroutines+2 {
		t.Errorf("Potential goroutine leak detected: initial=%d, final=%d", initialGoroutines, finalGoroutines)
	}
}

// TestDoNormalOperation verifies that normal operation still works
func TestDoNormalOperation(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test response"))
	})

	req, err := http.NewRequest("GET", "/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := Do(handler, req)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}
