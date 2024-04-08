package beaconevents_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
)

func TestEmitterSet(t *testing.T) {
	e := beaconevents.NewEmitters()
	var called int
	_, err := e.Subscribe([]string{"set"}, func(topic string, item any) {
		require.EqualValues(t, "set", topic)
		require.EqualValues(t, "hello", item.(string))
		called = called + 1
	})
	require.NoError(t, err)
	e.Publish("set", "hello")
	require.EqualValues(t, 1, called)
}
func TestEmitterFilters(t *testing.T) {
	e := beaconevents.NewEmitters()
	var a atomic.Int64
	var b atomic.Int64
	var ab atomic.Int64
	var wild atomic.Int64
	_, err := e.Subscribe([]string{"a"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		a.Add(1)
	})
	require.NoError(t, err)
	_, err = e.Subscribe([]string{"b"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		b.Add(1)
	})
	require.NoError(t, err)
	_, err = e.Subscribe([]string{"a", "b"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		ab.Add(1)
	})
	require.NoError(t, err)
	_, err = e.Subscribe([]string{"*"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		wild.Add(1)
	})
	require.NoError(t, err)

	e.Publish("a", "a")
	e.Publish("b", "b")
	e.Publish("b", "b")
	e.Publish("c", "c")

	require.EqualValues(t, 1, a.Load())
	require.EqualValues(t, 2, b.Load())
	require.EqualValues(t, 3, ab.Load())
	require.EqualValues(t, 4, wild.Load())
}
