// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from: https://golang.org/src/crypto/cipher/xor_test.go

package bitutil

import (
	"bytes"
	"testing"
)

// Tests that bitwise AND works for various alignments.
func TestAND(t *testing.T) {
	for alignP := range 2 {
		for alignQ := range 2 {
			for alignD := range 2 {
				p := make([]byte, 1023)[alignP:]
				q := make([]byte, 1023)[alignQ:]

				for i := range p {
					p[i] = byte(i)
				}
				for i := range q {
					q[i] = byte(len(q) - i)
				}
				d1 := make([]byte, 1023+alignD)[alignD:]
				d2 := make([]byte, 1023+alignD)[alignD:]

				ANDBytes(d1, p, q)
				safeANDBytes(d2, p, q)
				if !bytes.Equal(d1, d2) {
					t.Error("not equal")
				}
			}
		}
	}
}

// Tests that bitwise OR works for various alignments.
func TestOR(t *testing.T) {
	for alignP := range 2 {
		for alignQ := range 2 {
			for alignD := range 2 {
				p := make([]byte, 1023)[alignP:]
				q := make([]byte, 1023)[alignQ:]

				for i := range p {
					p[i] = byte(i)
				}
				for i := range q {
					q[i] = byte(len(q) - i)
				}
				d1 := make([]byte, 1023+alignD)[alignD:]
				d2 := make([]byte, 1023+alignD)[alignD:]

				ORBytes(d1, p, q)
				safeORBytes(d2, p, q)
				if !bytes.Equal(d1, d2) {
					t.Error("not equal")
				}
			}
		}
	}
}

// Tests that bit testing works for various alignments.
func TestTest(t *testing.T) {
	for align := range 2 {
		// Test for bits set in the bulk part
		p := make([]byte, 1023)[align:]
		p[100] = 1

		if TestBytes(p) != safeTestBytes(p) {
			t.Error("not equal")
		}
		// Test for bits set in the tail part
		q := make([]byte, 1023)[align:]
		q[len(q)-1] = 1

		if TestBytes(q) != safeTestBytes(q) {
			t.Error("not equal")
		}
	}
}

var GloBool bool // Exported global will not be dead-code eliminated, at least not yet.
