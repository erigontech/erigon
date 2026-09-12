package stagedsync

import (
	"context"
	"runtime"
	"slices"
	"strings"
)

// resultSink is one fan-out destination for the exec loop's result stream.
// feedsReadBase marks a consumer whose output gates the next block's read base.
type resultSink struct {
	name          string
	ch            chan applyResult
	feedsReadBase bool
	closed        bool // touched only by resultStream.close; publish never reads it
}

// resultStream is the exec loop's fan-out registry, owning result delivery to
// every registered consumer and the ordered shutdown of their channels. Sinks
// are registered in publish order; close walks them in reverse so the commitment
// calculator's channel closes before the apply loop's, or the trailing
// commitment write lands on a closed channel.
type resultStream struct {
	sinks []*resultSink
}

func newResultStream() *resultStream { return &resultStream{} }

// register adds a consumer's channel to the fan-out. A nil channel registers a
// disabled sink (skipped by publish/close/sendControl) so callers keep their
// wiring when a consumer is switched off.
func (s *resultStream) register(name string, ch chan applyResult, feedsReadBase bool) {
	s.sinks = append(s.sinks, &resultSink{name: name, ch: ch, feedsReadBase: feedsReadBase})
}

// publish fans a result out to every registered consumer. With mustDeliver it
// blocks unconditionally on each (the terminal blockResult must reach every
// consumer even after ctx is cancelled); otherwise it only honours ctx.Done once
// a consumer's buffer is full, so a truly-gone consumer cannot deadlock the
// producer. A send on a channel closed during shutdown is reported as
// context.Canceled.
func (s *resultStream) publish(ctx context.Context, r applyResult, mustDeliver bool) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(runtime.Error); ok && strings.Contains(e.Error(), "send on closed channel") {
				err = context.Canceled
				return
			}
			panic(rec)
		}
	}()
	for _, sink := range s.sinks {
		if sink.ch == nil {
			continue
		}
		if mustDeliver {
			sink.ch <- r
			continue
		}
		select {
		case sink.ch <- r:
		default:
			select {
			case sink.ch <- r:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// sendControl delivers an out-of-band message to a single named consumer,
// blocking until it lands. A closed target during shutdown drops the message.
func (s *resultStream) sendControl(name string, r applyResult) {
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(runtime.Error); ok && strings.Contains(e.Error(), "send on closed channel") {
				return
			}
			panic(rec)
		}
	}()
	for _, sink := range s.sinks {
		if sink.name == name && sink.ch != nil {
			sink.ch <- r
			return
		}
	}
}

// close closes every registered consumer's channel in reverse registration
// order and returns the names closed, in order. Safe to call repeatedly: an
// already-closed sink is skipped and a concurrent close by a racing shutdown
// path is recovered.
func (s *resultStream) close() (closedOrder []string) {
	for _, sink := range slices.Backward(s.sinks) {
		if sink.ch == nil || sink.closed {
			continue
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					if e, ok := rec.(runtime.Error); ok && strings.Contains(e.Error(), "close of closed channel") {
						sink.closed = true
						return
					}
					panic(rec)
				}
			}()
			close(sink.ch)
			sink.closed = true
			closedOrder = append(closedOrder, sink.name)
		}()
	}
	return
}
