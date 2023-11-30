package sse

import "io"

// Packet represents an event to send
// the order in this struct is the order that they will be sent.
type Packet struct {

	// as a special case, an empty value of event will not write an event header
	Event string

	// additional headers to be added.
	// using the reserved headers event, header, data, id is undefined behavior
	// note that this is the canonical way to send the "retry" header
	Header map[string]string

	// the io.Reader to source the data from
	Data io.Reader

	// whether or not to send an id, and if so, what id to send
	// a nil id means to not send an id.
	// empty string means to simply send the string "id\n"
	// otherwise, the id is sent as is
	// id is always sent at the end of the packet
	ID *string
}

func ID(x string) *string {
	return &x
}

// Encoder works at a higher level than the encoder.
// it works on the packet level.
type Encoder struct {
	wr *Writer

	firstWriteDone bool
}

func NewEncoder(w io.Writer) *Encoder {
	wr := NewWriter(w)
	return &Encoder{
		wr: wr,
	}
}

func (e *Encoder) Encode(p *Packet) error {
	if e.firstWriteDone {
		err := e.wr.Next()
		if err != nil {
			return err
		}
	}
	e.firstWriteDone = true
	if len(p.Event) > 0 {
		if err := e.wr.Header("event", p.Event); err != nil {
			return err
		}
	}
	if p.Header != nil {
		for k, v := range p.Header {
			if err := e.wr.Header(k, v); err != nil {
				return err
			}
		}
	}
	if p.Data != nil {
		if err := e.wr.WriteData(p.Data); err != nil {
			return err
		}
	}
	err := e.wr.Flush()
	if err != nil {
		return err
	}
	if p.ID != nil {
		if err := e.wr.Header("id", *p.ID); err != nil {
			return err
		}
	}
	return nil
}
