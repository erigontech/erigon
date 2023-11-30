package sse

import (
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

type writeState struct {
	inMessage        bool
	trailingCarriage bool
}

// writer is not thread safe. it is meant for internal usage
type Writer struct {
	raw io.Writer

	es writeState

	w io.Writer

	o Options
}

func NewWriter(w io.Writer, opts ...Option) *Writer {
	o := &Options{}
	for _, v := range opts {
		v(o)
	}
	return &Writer{
		raw: w,
		w:   w,
		o:   *o,
	}
}

func (e *Writer) writeByte(x byte) error {
	_, err := e.w.Write([]byte{x})
	return err
}
func (e *Writer) writeString(s string) (int, error) {
	return e.w.Write([]byte(s))
}

func (e *Writer) Flush() error {
	if e.es.inMessage {
		// we are in a message, so write a newline to terminate it, as the user did not
		err := e.writeByte('\n')
		if err != nil {
			return err
		}
		e.es.inMessage = false
	}
	// and reset the trailingCarriage state as well
	e.es.trailingCarriage = false
	return nil
}

// next should be called at the end of an event. it will call Flush and then write a newline
func (e *Writer) Next() error {

	if err := e.Flush(); err != nil {
		return err
	}
	// we write a newline, indicating now that this is a new event
	if err := e.writeByte('\n'); err != nil {
		return err
	}
	return nil
}

// Event will start writing an event with the name topic to the stream
func (e *Writer) Header(name string, topic string) error {
	if topic == "" {
		return nil
	}
	if e.o.ValidateUTF8() {
		if !utf8.ValidString(topic) {
			return ErrInvalidUTF8Bytes
		}
	}
	if len(topic) > 0 {
		if _, err := e.writeString(name + ": "); err != nil {
			return err
		}
		// write the supplied topic
		if _, err := e.writeString(topic); err != nil {
			return err
		}
	}
	if err := e.writeByte('\n'); err != nil {
		return err
	}

	return nil
}

// a convenient wrapper for writing data from io.Reader so that one can easily replay events.
func (e *Writer) WriteData(r io.Reader) (err error) {
	if _, err = io.Copy(e, r); err != nil {
		return err
	}
	return
}

// Write underlying write method for piping data. be careful using this!
func (e *Writer) Write(xs []byte) (n int, err error) {
	if e.o.ValidateUTF8() && !utf8.Valid(xs) {
		return 0, ErrInvalidUTF8Bytes
	}
	for _, x := range xs {
		// now, see if there was a trailing carriage left over from the last write
		// only check and write the data if we are do not have a trailing carriage
		if !e.es.trailingCarriage {
			e.checkMessage()
		}
		if e.es.trailingCarriage {
			// if there is, see if the character is a newline
			if x != '\n' {
				// its not a newline, so the trailing carriage was a valid end of message. write a new data field
				e.es.inMessage = false
				e.checkMessage()
			}
			// in the case that the character is a newline
			// we will just write the newline and inMessage=false will be set in the case below

			// in both cases, the trailing carriage is dealt with
			e.es.trailingCarriage = false
		}
		// write the byte no matter what
		err = e.writeByte(x)
		if err != nil {
			return
		}
		// if success, note that we wrote another byte
		n++
		if x == '\n' {
			// end message if it's a newline always
			e.es.inMessage = false
		} else if x == '\r' {
			// if x is a carriage return, mark it as trailing carriage
			e.es.trailingCarriage = true
			e.es.inMessage = false
		}
	}
	return
}

func (e *Writer) checkMessage() {
	if !e.es.inMessage {
		e.es.inMessage = true
		e.writeString("data: ")
	}
}
