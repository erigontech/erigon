package rlp

import "golang.org/x/exp/constraints"

type EncoderFunc = func(i *Encoder) *Encoder

type Encoder struct {
	buf []byte
}

func NewEncoder(buf []byte) *Encoder {
	return &Encoder{
		buf: buf,
	}
}

// Buffer returns the underlying buffer
func (e *Encoder) Buffer() []byte {
	return e.buf
}

func (e *Encoder) Byte(p byte) *Encoder {
	e.buf = append(e.buf, p)
	return e
}

func (e *Encoder) Bytes(p []byte) *Encoder {
	e.buf = append(e.buf, p...)
	return e
}

// Str will write a string correctly
func (e *Encoder) Str(str []byte) *Encoder {
	if len(str) > 55 {
		return e.LongString(str)
	}
	return e.ShortString(str)
}

// String will assume your string is less than 56 bytes long, and do no validation as such
func (e *Encoder) ShortString(str []byte) *Encoder {
	return e.Byte(TokenShortBlob.Plus(byte(len(str)))).Bytes(str)
}

// String will assume your string is greater than 55 bytes long, and do no validation as such
func (e *Encoder) LongString(str []byte) *Encoder {
	// write the indicator token
	e.Byte(byte(TokenLongBlob))
	// write the integer, knowing that we appended n bytes
	n := putUint(e, len(str))
	// so we knw the indicator token was n+1 bytes ago.
	e.buf[len(e.buf)-(int(n)+1)] += n
	// and now add the actual length
	e.buf = append(e.buf, str...)
	return e
}

// List will attempt to write the list of encoder funcs to the buf
func (e *Encoder) List(items ...EncoderFunc) *Encoder {
	return e.writeList(true, items...)
}

// ShortList actually calls List
func (e *Encoder) ShortList(items ...EncoderFunc) *Encoder {
	return e.writeList(true, items...)
}

// LongList will assume that your list payload is more than 55 bytes long, and do no validation as such
func (e *Encoder) LongList(items ...EncoderFunc) *Encoder {
	return e.writeList(false, items...)
}

// writeList will first attempt to write a long list with the dat
// if validate is false, it will just format it like the length is above 55
// if validate is true, it will format it like it is a shrot list
func (e *Encoder) writeList(validate bool, items ...EncoderFunc) *Encoder {
	// write the indicator token
	e = e.Byte(byte(TokenLongList))
	// now pad 8 bytes
	e = e.Bytes(make([]byte, 8))
	// record the length before encoding items
	startLength := len(e.buf)
	// now write all the items
	for _, v := range items {
		e = v(e)
	}
	// the size is the difference in the lengths now
	dataSize := len(e.buf) - startLength
	if dataSize <= 55 && validate {
		// oh it's actually a short string! awkward. let's set that then.
		e.buf[startLength-8-1] = TokenShortList.Plus(byte(dataSize))
		// and then copy the data over
		copy(e.buf[startLength-8:], e.buf[startLength:startLength+dataSize])
		// and now set the new size
		e.buf = e.buf[:startLength+dataSize-8]
		// we are done, return
		return e
	}
	// ok, so it's a long string.
	// create a new encoder centered at startLength - 8
	enc := NewEncoder(e.buf[startLength-8:])
	// now write using that encoder the size
	n := putUint(enc, dataSize)
	// and update the token, which we know is at startLength-8-1
	e.buf[startLength-8-1] += n
	// the shift to perform now is 8 - n.
	shift := int(8 - n)
	// if there is a positive shift, then we must perform the shift
	if shift > 0 {
		// copy the data
		copy(e.buf[startLength-shift:], e.buf[startLength:startLength+dataSize])
		// set the new length
		e.buf = e.buf[:startLength-shift+dataSize]
	}
	return e
}

func putUint[T constraints.Integer](e *Encoder, t T) (size byte) {
	i := uint64(t)
	switch {
	case i < (1 << 8):
		e.buf = append(e.buf, byte(i))
		return 1
	case i < (1 << 16):
		e.buf = append(e.buf,
			byte(i>>8),
			byte(i),
		)
		return 2
	case i < (1 << 24):

		e.buf = append(e.buf,
			byte(i>>16),
			byte(i>>8),
			byte(i),
		)
		return 3
	case i < (1 << 32):
		e.buf = append(e.buf,
			byte(i>>24),
			byte(i>>16),
			byte(i>>8),
			byte(i),
		)
		return 4
	case i < (1 << 40):
		e.buf = append(e.buf,
			byte(i>>32),
			byte(i>>24),
			byte(i>>16),
			byte(i>>8),
			byte(i),
		)
		return 5
	case i < (1 << 48):
		e.buf = append(e.buf,
			byte(i>>40),
			byte(i>>32),
			byte(i>>24),
			byte(i>>16),
			byte(i>>8),
			byte(i),
		)
		return 6
	case i < (1 << 56):
		e.buf = append(e.buf,
			byte(i>>48),
			byte(i>>40),
			byte(i>>32),
			byte(i>>24),
			byte(i>>16),
			byte(i>>8),
			byte(i),
		)
		return 7
	default:
		e.buf = append(e.buf,
			byte(i>>56),
			byte(i>>48),
			byte(i>>40),
			byte(i>>32),
			byte(i>>24),
			byte(i>>16),
			byte(i>>8),
			byte(i),
		)
		return 8
	}
}
