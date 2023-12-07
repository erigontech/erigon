package sse

import (
	"bufio"
	"net/http"
	"strings"
)

// EventSink tracks a event source connection between a client and a server
type EventSink struct {
	wr  http.ResponseWriter
	r   *http.Request
	bw  *bufio.Writer
	enc *Encoder

	LastEventId string
}

func Upgrade(wr http.ResponseWriter, r *http.Request) (*EventSink, error) {
	if !strings.EqualFold(r.Header.Get("Content-Type"), "text/event-stream") {
		return nil, ErrInvalidContentType
	}
	o := &EventSink{
		wr: wr,
		r:  r,
		bw: bufio.NewWriter(wr),
	}
	o.LastEventId = r.Header.Get("Last-Event-ID")
	wr.Header().Add("Content-Type", "text/event-stream")
	o.enc = NewEncoder(o.bw)
	return o, nil
}

func (e *EventSink) Encode(p *Packet) error {
	err := e.enc.Encode(p)
	if err != nil {
		return err
	}
	return e.bw.Flush()
}
