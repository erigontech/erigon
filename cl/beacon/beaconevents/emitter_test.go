package beaconevents_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/stretchr/testify/require"
)

func TestEmitterSet(t *testing.T) {

	e := beaconevents.NewEmitters()
	var called int
	e.Subscribe([]string{"set"}, func(topic string, item any) {
		require.EqualValues(t, "set", topic)
		require.EqualValues(t, "hello", item.(string))
		called = called + 1
	})
	e.Publish("set", "hello")
	require.EqualValues(t, 1, called)
}
