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

// Package validation defines the validator chain that gates a
// downloaded snapshot file's promotion into the locally-trusted view.
// Validation lives inside Storage (per
// feature-pluggable-validation-phase): the orchestrator hands bytes
// off via Storage.RecordFile, the storage runs the configured chain,
// and only on success does the file become visible to readers.
//
// Stage 1 covers per-file checks runnable on a single file (hash
// match, well-formed for kind, content shape parses). Stage 2 covers
// cross-file / range / system-level checks (commitment chain, merge
// equivalence, history-vs-kv alignment). The same Validator interface
// serves both stages — stage 2 validators just demand more from the
// store-context they're given.
//
// Both consumer (downloaded-from-peer) and producer (locally-built)
// paths run the same chain. The producer side gains an additional
// user-defined plugin slot for deployment-specific gates; the
// validator interface is identical.
package validation

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Validator is a single check against a (file metadata, content) pair.
// Validators are stateless and safe to call concurrently — the chain
// may invoke them on independent files in parallel in future. Returns
// nil on pass; a descriptive error on reject.
//
// Stage-1 validators that don't need bytes ignore the content
// argument. Stage-2 validators may also need a store-read view; that
// is plumbed via separate context (not yet wired) when stage 2 lands.
type Validator interface {
	// Name is a stable identifier (used in error wrapping, log
	// messages, and future PeerPenalized event payloads).
	Name() string
	// Validate returns nil on accept. On reject, the wrapped error
	// should be operator-readable AND structured enough for future
	// peer-penalty / range-unavailable bookkeeping.
	Validate(file *snapshot.FileEntry, content ContentSource) error
}

// ContentSource yields the bytes associated with a file under
// validation. Implementations may be on-disk (FileContent for a path
// under snap-dir) or in-memory (BytesContent for tests). Open is
// allowed to be called multiple times — validators that need to
// stream the content independently can do so without coordinating.
//
// A nil ContentSource is permitted when no validator in the chain
// needs bytes. Validators that DO need bytes return a structured
// error if Open returns nil — the caller's bug, not a validation
// failure.
type ContentSource interface {
	Open() (io.ReadCloser, error)
}

// FileContent is a ContentSource backed by an on-disk file. The path
// is the absolute or snap-dir-relative location the storage layer
// resolved for the file under validation.
type FileContent struct{ Path string }

// Open returns a fresh reader for the file. The caller closes.
func (f FileContent) Open() (io.ReadCloser, error) {
	if f.Path == "" {
		return nil, fmt.Errorf("FileContent.Open: empty path")
	}
	return os.Open(f.Path)
}

// BytesContent is a ContentSource backed by an in-memory byte slice.
// Used by tests that want to feed validators canned bytes without
// touching the filesystem.
type BytesContent []byte

// Open returns a no-op-close reader over the underlying bytes.
func (b BytesContent) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b)), nil
}

// Chain runs validators in order, returning the first failure with
// the validator's name wrapped around the underlying error. An empty
// chain accepts everything (the trust-everyone shape that matches the
// pre-validation behaviour).
type Chain []Validator

// Validate runs every validator in chain order. Stops on first
// failure. The returned error is wrapped with the failing validator's
// Name so callers can attribute the rejection without unwrapping.
func (c Chain) Validate(file *snapshot.FileEntry, content ContentSource) error {
	for _, v := range c {
		if err := v.Validate(file, content); err != nil {
			return fmt.Errorf("%s: %w", v.Name(), err)
		}
	}
	return nil
}
