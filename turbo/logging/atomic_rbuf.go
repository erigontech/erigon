package logging

// AtomicFixedSizeRingBuf: Synchronized version of FixedSizeRingBuf,
// safe for concurrent access.
//
// copyright (c) 2014, Jason E. Aten
// license: MIT
//
// Some text from the Golang standard library doc is adapted and
// reproduced in fragments below to document the expected behaviors
// of the interface functions Read()/Write()/ReadFrom()/WriteTo() that
// are implemented here. Those descriptions (see
// http://golang.org/pkg/io/#Reader for example) are
// copyright 2010 The Go Authors.

import (
	"fmt"
	"io"
	"sync"
)

// AtomicFixedSizeRingBuf: see FixedSizeRingBuf for the full
// details; this is the same, just safe for current access
// (and thus paying the price of synchronization on each call
// as well.)
type AtomicFixedSizeRingBuf struct {
	A        [2][]byte // a pair of ping/pong buffers. Only one is active.
	Use      int       // which A buffer is in active use, 0 or 1
	N        int       // MaxViewInBytes, the size of A[0] and A[1] in bytes.
	Beg      int       // start of data in A[Use]
	readable int       // number of bytes available to read in A[Use]
	tex      sync.Mutex
}

// Readable() returns the number of bytes available for reading.
func (b *AtomicFixedSizeRingBuf) Readable() int {
	b.tex.Lock()
	defer b.tex.Unlock()
	return b.readable
}

// ContigLen gets the length of the largest read that we can provide to a contiguous slice
// without an extra linearizing copy of all bytes internally.
func (b *AtomicFixedSizeRingBuf) ContigLen() int {
	b.tex.Lock()
	defer b.tex.Unlock()

	extent := b.Beg + b.readable
	firstContigLen := intMin2(extent, b.N) - b.Beg
	return firstContigLen
}

// constructor. NewAtomicFixedSizeRingBuf will allocate internally
// two buffers of size maxViewInBytes.
func NewAtomicFixedSizeRingBuf(maxViewInBytes int) *AtomicFixedSizeRingBuf {
	n := maxViewInBytes
	r := &AtomicFixedSizeRingBuf{
		Use: 0, // 0 or 1, whichever is actually in use at the moment.
		// If we are asked for Bytes() and we wrap, linearize into the other.

		N:        n,
		Beg:      0,
		readable: 0,
	}
	r.A[0] = make([]byte, n, n)
	r.A[1] = make([]byte, n, n)

	return r
}

// Bytes() returns a slice of the contents of the unread portion of the buffer.
//
// To avoid copying, see the companion BytesTwo() call.
//
// Unlike the standard library Bytes() method (on bytes.Buffer for example),
// the result of the AtomicFixedSizeRingBuf::Bytes(true) is a completely new
// returned slice, so modifying that slice will have no impact on the contents
// of the internal ring.
//
// Bytes(false) acts like the standard library bytes.Buffer::Bytes() call,
// in that it returns a slice which is backed by the buffer itself (so
// no copy is involved).
//
// The largest slice Bytes ever returns is bounded above by the maxViewInBytes
// value used when calling NewAtomicFixedSizeRingBuf().
//
// Possible side-effect: may modify b.Use, the buffer in use.
func (b *AtomicFixedSizeRingBuf) Bytes(makeCopy bool) []byte {
	b.tex.Lock()
	defer b.tex.Unlock()

	extent := b.Beg + b.readable
	if extent <= b.N {
		// we fit contiguously in this buffer without wrapping to the other
		return b.A[b.Use][b.Beg:(b.Beg + b.readable)]
	}

	// wrap into the other buffer
	src := b.Use
	dest := 1 - b.Use

	n := copy(b.A[dest], b.A[src][b.Beg:])
	n += copy(b.A[dest][n:], b.A[src][0:(extent%b.N)])

	b.Use = dest
	b.Beg = 0

	if makeCopy {
		ret := make([]byte, n)
		copy(ret, b.A[b.Use][:n])
		return ret
	}
	return b.A[b.Use][:n]
}

// TwoBuffers: the return value of BytesTwo(). TwoBuffers
// holds two slices to the contents of the readable
// area of the internal buffer. The slices contents are logically
// ordered First then Second, but the Second will actually
// be physically before the First. Either or both of
// First and Second may be empty slices.
type TwoBuffers struct {
	First  []byte // the first part of the contents
	Second []byte // the second part of the contents
}

// BytesTwo returns all readable bytes, but in two separate slices,
// to avoid copying. The two slices are from the same buffer, but
// are not contiguous. Either or both may be empty slices.
func (b *AtomicFixedSizeRingBuf) BytesTwo() TwoBuffers {
	b.tex.Lock()
	defer b.tex.Unlock()
	return b.unatomic_BytesTwo()
}

func (b *AtomicFixedSizeRingBuf) unatomic_BytesTwo() TwoBuffers {
	extent := b.Beg + b.readable
	if extent <= b.N {
		// we fit contiguously in this buffer without wrapping to the other.
		// Let second stay an empty slice.
		return TwoBuffers{First: b.A[b.Use][b.Beg:(b.Beg + b.readable)], Second: []byte{}}
	}

	return TwoBuffers{First: b.A[b.Use][b.Beg:(b.Beg + b.readable)], Second: b.A[b.Use][0:(extent % b.N)]}
}

// Purpose of BytesTwo() and AdvanceBytesTwo(): avoid extra copying of data.
//
// AdvanceBytesTwo() takes a TwoBuffers as input, this must have been
// from a previous call to BytesTwo(); no intervening calls to Bytes()
// or Adopt() are allowed (or any other future routine or client data
// access that changes the internal data location or contents) can have
// been made.
//
// After sanity checks, AdvanceBytesTwo() advances the internal buffer, effectively
// calling Advance( len(tb.First) + len(tb.Second)).
//
// If intervening-calls that changed the buffers (other than appending
// data to the buffer) are detected, we will panic as a safety/sanity/
// aid-to-debugging measure.
func (b *AtomicFixedSizeRingBuf) AdvanceBytesTwo(tb TwoBuffers) {
	b.tex.Lock()
	defer b.tex.Unlock()

	tblen := len(tb.First) + len(tb.Second)

	if tblen == 0 {
		return // nothing to do
	}

	// sanity check: insure we have re-located in the meantime
	if tblen > b.readable {
		panic(fmt.Sprintf("tblen was %d, and this was greater than b.readerable = %d. Usage error detected and data loss may have occurred (available data appears to have shrunken out from under us!).", tblen, b.readable))
	}

	tbnow := b.unatomic_BytesTwo()

	if len(tb.First) > 0 {
		if tb.First[0] != tbnow.First[0] {
			panic(fmt.Sprintf("slice contents of First have changed out from under us!: '%s' vs '%s'", string(tb.First), string(tbnow.First)))
		}
	}
	if len(tb.Second) > 0 {
		if len(tb.First) > len(tbnow.First) {
			panic(fmt.Sprintf("slice contents of Second have changed out from under us! tbnow.First length(%d) is less than tb.First(%d.", len(tbnow.First), len(tb.First)))
		}
		if len(tbnow.Second) == 0 {
			panic(fmt.Sprintf("slice contents of Second have changed out from under us! tbnow.Second is empty, but tb.Second was not"))
		}
		if tb.Second[0] != tbnow.Second[0] {
			panic(fmt.Sprintf("slice contents of Second have changed out from under us!: '%s' vs '%s'", string(tb.Second), string(tbnow.Second)))
		}
	}

	b.unatomic_advance(tblen)
}

// Read():
//
// From bytes.Buffer.Read(): Read reads the next len(p) bytes
// from the buffer or until the buffer is drained. The return
// value n is the number of bytes read. If the buffer has no data
// to return, err is io.EOF (unless len(p) is zero); otherwise it is nil.
//
//  from the description of the Reader interface,
//     http://golang.org/pkg/io/#Reader
//
/*
Reader is the interface that wraps the basic Read method.

Read reads up to len(p) bytes into p. It returns the number
of bytes read (0 <= n <= len(p)) and any error encountered.
Even if Read returns n < len(p), it may use all of p as scratch
space during the call. If some data is available but not
len(p) bytes, Read conventionally returns what is available
instead of waiting for more.

When Read encounters an error or end-of-file condition after
successfully reading n > 0 bytes, it returns the number of bytes
read. It may return the (non-nil) error from the same call or
return the error (and n == 0) from a subsequent call. An instance
of this general case is that a Reader returning a non-zero number
of bytes at the end of the input stream may return
either err == EOF or err == nil. The next Read should
return 0, EOF regardless.

Callers should always process the n > 0 bytes returned before
considering the error err. Doing so correctly handles I/O errors
that happen after reading some bytes and also both of the
allowed EOF behaviors.

Implementations of Read are discouraged from returning a zero
byte count with a nil error, and callers should treat that
situation as a no-op.
*/
//
func (b *AtomicFixedSizeRingBuf) Read(p []byte) (n int, err error) {
	return b.ReadAndMaybeAdvance(p, true)
}

// ReadWithoutAdvance(): if you want to Read the data and leave
// it in the buffer, so as to peek ahead for example.
func (b *AtomicFixedSizeRingBuf) ReadWithoutAdvance(p []byte) (n int, err error) {
	return b.ReadAndMaybeAdvance(p, false)
}

func (b *AtomicFixedSizeRingBuf) ReadAndMaybeAdvance(p []byte, doAdvance bool) (n int, err error) {
	b.tex.Lock()
	defer b.tex.Unlock()

	if len(p) == 0 {
		return 0, nil
	}
	if b.readable == 0 {
		return 0, io.EOF
	}
	extent := b.Beg + b.readable
	if extent <= b.N {
		n += copy(p, b.A[b.Use][b.Beg:extent])
	} else {
		n += copy(p, b.A[b.Use][b.Beg:b.N])
		if n < len(p) {
			n += copy(p[n:], b.A[b.Use][0:(extent%b.N)])
		}
	}
	if doAdvance {
		b.unatomic_advance(n)
	}
	return
}

// Write writes len(p) bytes from p to the underlying data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// Write must return a non-nil error if it returns n < len(p).
//
// Write doesn't modify b.User, so once a []byte is pinned with
// a call to Bytes(), it should remain valid even with additional
// calls to Write() that come after the Bytes() call.
func (b *AtomicFixedSizeRingBuf) Write(p []byte) (n int, err error) {
	b.tex.Lock()
	defer b.tex.Unlock()

	for {
		if len(p) == 0 {
			// nothing (left) to copy in; notice we shorten our
			// local copy p (below) as we read from it.
			return
		}

		writeCapacity := b.N - b.readable
		if writeCapacity <= 0 {
			// we are all full up already.
			return n, io.ErrShortWrite
		}
		if len(p) > writeCapacity {
			err = io.ErrShortWrite
			// leave err set and
			// keep going, write what we can.
		}

		writeStart := (b.Beg + b.readable) % b.N

		upperLim := intMin2(writeStart+writeCapacity, b.N)

		k := copy(b.A[b.Use][writeStart:upperLim], p)

		n += k
		b.readable += k
		p = p[k:]

		// we can fill from b.A[b.Use][0:something] from
		// p's remainder, so loop
	}
}

// WriteTo and ReadFrom avoid intermediate allocation and copies.

// WriteTo avoids intermediate allocation and copies.
// WriteTo writes data to w until there's no more data to write
// or when an error occurs. The return value n is the number of
// bytes written. Any error encountered during the write is also returned.
func (b *AtomicFixedSizeRingBuf) WriteTo(w io.Writer) (n int64, err error) {
	b.tex.Lock()
	defer b.tex.Unlock()

	if b.readable == 0 {
		return 0, io.EOF
	}

	extent := b.Beg + b.readable
	firstWriteLen := intMin2(extent, b.N) - b.Beg
	secondWriteLen := b.readable - firstWriteLen
	if firstWriteLen > 0 {
		m, e := w.Write(b.A[b.Use][b.Beg:(b.Beg + firstWriteLen)])
		n += int64(m)
		b.unatomic_advance(m)

		if e != nil {
			return n, e
		}
		// all bytes should have been written, by definition of
		// Write method in io.Writer
		if m != firstWriteLen {
			return n, io.ErrShortWrite
		}
	}
	if secondWriteLen > 0 {
		m, e := w.Write(b.A[b.Use][0:secondWriteLen])
		n += int64(m)
		b.unatomic_advance(m)

		if e != nil {
			return n, e
		}
		// all bytes should have been written, by definition of
		// Write method in io.Writer
		if m != secondWriteLen {
			return n, io.ErrShortWrite
		}
	}

	return n, nil
}

// ReadFrom avoids intermediate allocation and copies.
// ReadFrom() reads data from r until EOF or error. The return value n
// is the number of bytes read. Any error except io.EOF encountered
// during the read is also returned.
func (b *AtomicFixedSizeRingBuf) ReadFrom(r io.Reader) (n int64, err error) {
	b.tex.Lock()
	defer b.tex.Unlock()

	for {
		writeCapacity := b.N - b.readable
		if writeCapacity <= 0 {
			// we are all full
			return n, nil
		}
		writeStart := (b.Beg + b.readable) % b.N
		upperLim := intMin2(writeStart+writeCapacity, b.N)

		m, e := r.Read(b.A[b.Use][writeStart:upperLim])
		n += int64(m)
		b.readable += m
		if e == io.EOF {
			return n, nil
		}
		if e != nil {
			return n, e
		}
	}
}

// Reset quickly forgets any data stored in the ring buffer. The
// data is still there, but the ring buffer will ignore it and
// overwrite those buffers as new data comes in.
func (b *AtomicFixedSizeRingBuf) Reset() {
	b.tex.Lock()
	defer b.tex.Unlock()

	b.Beg = 0
	b.readable = 0
	b.Use = 0
}

// Advance(): non-standard, but better than Next(),
// because we don't have to unwrap our buffer and pay the cpu time
// for the copy that unwrapping may need.
// Useful in conjuction/after ReadWithoutAdvance() above.
func (b *AtomicFixedSizeRingBuf) Advance(n int) {
	b.tex.Lock()
	defer b.tex.Unlock()

	b.unatomic_advance(n)
}

// unatomic_advance(): private implementation of Advance() without
// the locks. See Advance() above for description.
// Necessary so that other methods that already hold
// locks can advance, and there are no recursive mutexes
// in Go.
func (b *AtomicFixedSizeRingBuf) unatomic_advance(n int) {
	if n <= 0 {
		return
	}
	if n > b.readable {
		n = b.readable
	}
	b.readable -= n
	b.Beg = (b.Beg + n) % b.N
}

// Adopt(): non-standard.
//
// For efficiency's sake, (possibly) take ownership of
// already allocated slice offered in me.
//
// If me is large we will adopt it, and we will potentially then
// write to the me buffer.
// If we already have a bigger buffer, copy me into the existing
// buffer instead.
//
// Side-effect: may change b.Use, among other internal state changes.
func (b *AtomicFixedSizeRingBuf) Adopt(me []byte) {
	b.tex.Lock()
	defer b.tex.Unlock()

	n := len(me)
	if n > b.N {
		b.A[0] = me
		b.A[1] = make([]byte, n, n)
		b.N = n
		b.Use = 0
		b.Beg = 0
		b.readable = n
	} else {
		// we already have a larger buffer, reuse it.
		copy(b.A[0], me)
		b.Use = 0
		b.Beg = 0
		b.readable = n
	}
}

// keep the atomic_rbuf.go standalone and usable without
// the rbuf.go file, by simply duplicating intMin from rbuf.go
func intMin2(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
