package stagedsync

import (
	"context"
	"errors"
	"testing"
)

type recordingConsumer struct {
	name     string
	feeds    bool
	log      *[]string
	failOnTx error
}

func (c *recordingConsumer) Name() string        { return c.name }
func (c *recordingConsumer) FeedsReadBase() bool { return c.feeds }
func (c *recordingConsumer) Open(context.Context) error {
	*c.log = append(*c.log, "open:"+c.name)
	return nil
}
func (c *recordingConsumer) OnTxResult(context.Context, *txResult) error {
	*c.log = append(*c.log, "tx:"+c.name)
	return c.failOnTx
}
func (c *recordingConsumer) OnBlockEnd(context.Context, *blockResult) error {
	*c.log = append(*c.log, "block:"+c.name)
	return nil
}
func (c *recordingConsumer) Close(error) error {
	*c.log = append(*c.log, "close:"+c.name)
	return nil
}

func newPipeline(log *[]string, names ...string) *consumerPipeline {
	p := &consumerPipeline{}
	for _, n := range names {
		p.consumers = append(p.consumers, &recordingConsumer{name: n, log: log})
	}
	return p
}

func TestConsumerPipelineDeliversInRegistrationOrder(t *testing.T) {
	var log []string
	p := newPipeline(&log, "domain", "log")
	ctx := context.Background()
	if err := p.onTx(ctx, &txResult{}); err != nil {
		t.Fatalf("onTx: %v", err)
	}
	if err := p.onBlock(ctx, &blockResult{}); err != nil {
		t.Fatalf("onBlock: %v", err)
	}
	want := []string{"tx:domain", "tx:log", "block:domain", "block:log"}
	if len(log) != len(want) {
		t.Fatalf("got %v, want %v", log, want)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("order[%d]=%q, want %q (full %v)", i, log[i], want[i], log)
		}
	}
}

func TestConsumerPipelineClosesInReverseOrder(t *testing.T) {
	var log []string
	p := newPipeline(&log, "domain", "log")
	if err := p.closeAll(nil); err != nil {
		t.Fatalf("closeAll: %v", err)
	}
	want := []string{"close:log", "close:domain"}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("close order[%d]=%q, want %q (full %v)", i, log[i], want[i], log)
		}
	}
}

func TestConsumerPipelineOnTxStopsAtFirstError(t *testing.T) {
	var log []string
	boom := errors.New("boom")
	p := &consumerPipeline{consumers: []ResultConsumer{
		&recordingConsumer{name: "domain", log: &log, failOnTx: boom},
		&recordingConsumer{name: "log", log: &log},
	}}
	err := p.onTx(context.Background(), &txResult{})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	// log consumer must NOT run after domain failed.
	if len(log) != 1 || log[0] != "tx:domain" {
		t.Fatalf("expected only tx:domain, got %v", log)
	}
}
