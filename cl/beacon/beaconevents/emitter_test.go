// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package beaconevents_test

import (
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
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
func TestEmitterFilters(t *testing.T) {
	e := beaconevents.NewEmitters()
	var a atomic.Int64
	var b atomic.Int64
	var ab atomic.Int64
	var wild atomic.Int64
	e.Subscribe([]string{"a"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		a.Add(1)
	})
	e.Subscribe([]string{"b"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		b.Add(1)
	})
	e.Subscribe([]string{"a", "b"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		ab.Add(1)
	})
	e.Subscribe([]string{"*"}, func(topic string, item any) {
		require.EqualValues(t, topic, item.(string))
		wild.Add(1)
	})

	e.Publish("a", "a")
	e.Publish("b", "b")
	e.Publish("b", "b")
	e.Publish("c", "c")

	require.EqualValues(t, 1, a.Load())
	require.EqualValues(t, 2, b.Load())
	require.EqualValues(t, 3, ab.Load())
	require.EqualValues(t, 4, wild.Load())
}
