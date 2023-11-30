package sse

import (
	"bytes"
	"io"
	"unicode/utf8"
	//"github.com/segmentio/asm/utf8" -- can switch to this library in the future if needed
)

type Option func(*Options)

func OptionValidateUtf8(enable bool) Option {
	return func(o *Options) {
		o.validateUTF8 = true
	}
}

type Options struct {
	validateUTF8 bool
}

func (e *Options) ValidateUTF8() bool {
	return e.validateUTF8
}

type encodeState struct {
	inMessage        bool
	trailingCarriage bool
}

// encoder is not thread safe :)
type Encoder struct {
	raw io.Writer
	buf bytes.Buffer

	es encodeState

	o *Options
}

func NewEncoder(w io.Writer, opts ...Option) *Encoder {
	o := &Options{}
	for _, v := range opts {
		v(o)
	}
	return &Encoder{
		raw: w,
		o:   o,
	}
}

func (e *Encoder) Write(xs []byte) (n int64, err error) {
	if e.o.ValidateUTF8() && !utf8.Valid(xs) {
		return 0, ErrInvalidUTF8Bytes
	}
	e.checkMessage()
	for _, x := range xs {
		// see if there was a trailing carriage left over from the last write
		if e.es.trailingCarriage {
			// if there is, see if the character is a newline
			if x != '\n' {
				// its not a newline, so we should flush first and create a new data entry
				err = e.flushMessage()
				if err != nil {
					return
				}
				e.checkMessage()
			}
			// in the case that the character is a newline
			// we will just write the newline and flush immediately after
			// in both cases, the trailing carriage is

			e.es.trailingCarriage = false
		}
		// write the byte no matter what
		e.buf.WriteByte(x)
		// flush if it is a newline always
		if x == '\n' {
			err = e.flushMessage()
			if err != nil {
				return
			}
		}
		// if x is a carriage return, mark it as trailing carriage
		if x == '\r' {
			e.es.trailingCarriage = true
		}
	}
	return 0, nil
}

func (e *Encoder) checkMessage() {
	if !e.es.inMessage {
		e.es.inMessage = true
		e.buf.WriteString("data: ")
	}
}

func (e *Encoder) flushMessage() error {
	_, err := e.buf.WriteTo(e.raw)
	if err != nil {
		return err
	}
	e.es.inMessage = false
	return nil

}
