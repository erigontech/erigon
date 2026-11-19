package stagedsync

import (
	"context"
	"slices"
)

// resultSink is one fan-out destination for the exec loop's result stream.
type resultSink struct {
	name string
	ch   chan applyResult
}

// resultStream is the exec loop's fan-out registry, owning result delivery to
// every registered consumer and the ordered shutdown of their channels. Sinks
// are registered in publish order; close walks them in reverse so the commitment
// calculator's channel closes before the apply loop's, or the trailing
// commitment write lands on a closed channel.
//
// publish, sendControl and close all run on the exec-loop goroutine, so a send
// after close or a double close cannot happen and needs no recover.
type resultStream struct {
	sinks []*resultSink
}

func newResultStream() *resultStream { return &resultStream{} }

// register adds a consumer's channel to the fan-out. A nil channel registers a
// disabled sink (skipped by publish/close/sendControl) so callers keep their
// wiring when a consumer is switched off.
func (s *resultStream) register(name string, ch chan applyResult) {
	s.sinks = append(s.sinks, &resultSink{name: name, ch: ch})
}

// publish fans a result out to every registered consumer. With mustDeliver it
// blocks unconditionally on each (the terminal blockResult must reach every
// consumer even after ctx is cancelled); otherwise it only honours ctx.Done once
// a consumer's buffer is full, so a truly-gone consumer cannot deadlock the
// producer.
func (s *resultStream) publish(ctx context.Context, r applyResult, mustDeliver bool) error {
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
// blocking until it lands.
func (s *resultStream) sendControl(name string, r applyResult) {
	for _, sink := range s.sinks {
		if sink.name == name && sink.ch != nil {
			sink.ch <- r
			return
		}
	}
}

// close closes every registered consumer's channel in reverse registration order
// and returns the names closed, in order.
func (s *resultStream) close() (closedOrder []string) {
	for _, sink := range slices.Backward(s.sinks) {
		if sink.ch == nil {
			continue
		}
		close(sink.ch)
		closedOrder = append(closedOrder, sink.name)
	}
	return
}
