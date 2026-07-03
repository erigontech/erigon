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

package commitment

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// tracePrefix tags each line (including interior lines) with its prefix and
// leaves the final trailing newline alone.
func TestTracePrefix_TagsWholeLines(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := tracePrefix(&buf, "[dead] ")
	fmt.Fprint(w, "one\n")
	fmt.Fprint(w, "multi\nline\n")
	require.Equal(t, "[dead] one\n[dead] multi\n[dead] line\n", buf.String())
}

func TestTracePrefix_NilDisables(t *testing.T) {
	t.Parallel()
	require.Nil(t, tracePrefix(nil, "[x] "))
}

// Concurrent fold workers (the parallel/streaming trie) each own a prefixWriter
// over one shared syncWriter; every emitted line must stay whole and carry its
// worker tag — never interleaved or corrupted mid-line.
func TestSyncWriter_ConcurrentLinesStayAttributed(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	shared := NewSyncWriter(&buf)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		w := tracePrefix(shared, fmt.Sprintf("[%x] ", g))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				fmt.Fprintf(w, "step %d\n", i)
			}
		}()
	}
	wg.Wait()

	line := regexp.MustCompile(`^\[[0-9a-f]\] step \d+$`)
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 8*250)
	for _, ln := range lines {
		require.Regexp(t, line, ln, "concurrent trace line was interleaved or lost its tag")
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

// prefixWriter must not silently accept a short underlying write.
func TestPrefixWriter_ShortWrite(t *testing.T) {
	t.Parallel()
	pw := &prefixWriter{w: shortWriter{}, prefix: []byte("[x] ")}
	_, err := pw.Write([]byte("hello\n"))
	require.ErrorIs(t, err, io.ErrShortWrite)
}
