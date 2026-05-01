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

// Package example_extension is a teaching artefact: a complete worked
// example of how to add a validator to the storage validation chain.
// See ../EXTENDING.md for the surrounding doc.
//
// This package is NOT used in production. It exists only so the
// pattern shown in EXTENDING.md is compilable and testable.
package example_extension

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// FileNameSuffixWhitelist accepts a file only if its name ends with
// one of the configured suffixes. Useful for an operator-side policy
// that rejects unexpected file kinds (e.g. allow only .seg, .kv, .v,
// reject any other extension that might appear).
//
// The struct holds configuration; Validate is a pure function of the
// (file, content) pair — no per-call state is mutated. This is the
// expected shape for a validator: configuration in fields, behaviour
// in Validate.
type FileNameSuffixWhitelist struct {
	// AllowedSuffixes is the list of acceptable file-name suffixes
	// (with leading dot, e.g. ".seg", ".kv"). Empty list rejects
	// every file.
	AllowedSuffixes []string
}

// Name implements validation.Validator. The string is used in error
// wrapping and log output; it should be stable across releases so
// operator dashboards stay coherent.
func (FileNameSuffixWhitelist) Name() string {
	return "filename_suffix_whitelist"
}

// Validate implements validation.Validator. Returns nil on accept,
// or a structured error on reject. The reject error includes the
// rejected name AND the acceptable set so operators can act on the
// log message without unwrapping.
//
// content is unused — this validator checks only metadata. Stage-1
// validators of this shape ignore the ContentSource.
func (v FileNameSuffixWhitelist) Validate(file *snapshot.FileEntry, _ validation.ContentSource) error {
	for _, allowed := range v.AllowedSuffixes {
		if strings.HasSuffix(file.Name, allowed) {
			return nil
		}
	}
	return fmt.Errorf(
		"file %q has no allowed suffix; expected one of %v",
		file.Name, v.AllowedSuffixes,
	)
}

// ExampleChain demonstrates how a consumer composes a chain that
// includes built-in validators alongside this extension. The
// composition is just listing types — there is no registry, no
// initialisation order, no plugin loader. The chain runs validators
// in the order they appear; each validator's Name appears in the
// error if it rejects.
func ExampleChain() validation.Chain {
	return validation.Chain{
		validation.NameNotEmpty{},
		validation.RangeOrdering{},
		FileNameSuffixWhitelist{
			AllowedSuffixes: []string{".seg", ".kv", ".v", ".idx", ".kvi", ".ef", ".efi"},
		},
	}
}
