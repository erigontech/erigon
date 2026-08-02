package stagedsync

import (
	"context"
	"runtime"
	"slices"
	"strings"
)

// resultSink is one fan-out destination for the exec loop's per-tx / per-block
// result stream. The registry owns the channel handle, the fan-out send and the
// ordered close, so a consumer implements only its own drain loop and declares
// whether it feeds the read base.
//
// feedsReadBase marks a consumer whose output gates the next block's read base
// (the domain updater, once apply moves off the exec loop): its applied frontier
// drives the read chain and backpressure. Every other consumer is a pure sink
// that lags freely behind its bounded channel and never gates a read. Recorded
// here as the seam later steps wire; the fan-out itself treats all sinks alike.
type resultSink struct {
	name          string
	ch            chan applyResult
	feedsReadBase bool
	closed        bool // touched only by resultStream.close; publish never reads it
}

// resultStream is the exec loop's fan-out registry: the single owner of result
// delivery to every registered consumer and of the ordered shutdown of their
// channels. It replaces the two hardcoded channels (applyResults, commitResults)
// with a registered set, so a new consumer joins the pipeline without editing
// the producer's fan-out or the close path.
//
// Sinks are registered in publish order; close walks them in reverse so the
// commitment calculator's channel closes before the apply loop's — the
// calculator must drain and close rootResults before the apply loop sees its
// own close, or the trailing commitment write lands on a closed channel.
type resultStream struct {
	sinks []*resultSink
}

func newResultStream() *resultStream { return &resultStream{} }

// register adds a consumer's channel to the fan-out. A nil channel registers a
// disabled sink (skipped by publish/close/sendControl) so callers keep their
// wiring when a consumer is switched off — e.g. DiscardCommitment nils the
// commit sink.
func (s *resultStream) register(name string, ch chan applyResult, feedsReadBase bool) {
	s.sinks = append(s.sinks, &resultSink{name: name, ch: ch, feedsReadBase: feedsReadBase})
}

// publish fans a result out to every registered consumer. With mustDeliver it
// blocks unconditionally on each (the terminal blockResult must reach every
// consumer even after the coordination ctx is cancelled); otherwise it delivers
// while a consumer's buffer has room and only honours ctx.Done once the buffer
// is full, so a truly-gone consumer cannot deadlock the producer. A send on a
// channel already closed during batch shutdown is benign and reported as
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
// blocking unconditionally until it lands (the calculator keeps draining until
// its channel closes, so this cannot wedge). Used for the batch-commitment
// trigger, which must reach the calculator even once the coordination ctx is
// cancelled. A closed target during shutdown drops the message.
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
// order (commit before apply) and returns the names closed, in order. Safe to
// call repeatedly: an already-closed sink is skipped, and a concurrent close of
// the same channel by a racing shutdown path is recovered. Channels with an
// external sole sender (blockRequests) are not registered here — their sender
// closes them.
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
