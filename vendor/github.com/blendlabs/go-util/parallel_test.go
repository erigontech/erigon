package util

import (
	"sync/atomic"
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestParallelAwait(t *testing.T) {
	assert := assert.New(t)
	firstDidRun := false
	secondDidRun := false
	thirdDidRun := false
	Parallel.Await(func() {
		firstDidRun = true
	}, func() {
		secondDidRun = true
	}, func() {
		thirdDidRun = true
	})

	assert.True(firstDidRun)
	assert.True(secondDidRun)
	assert.True(thirdDidRun)
}

func TestParallelEach(t *testing.T) {
	assert := assert.New(t)

	items := Sequence.Ints(10)
	var counter int32

	// trivial test, counter should equal 10.
	Parallel.Each(items, 2, func(v interface{}) {
		atomic.AddInt32(&counter, 1)
	})
	assert.Equal(counter, len(items))
}

func TestParallelEachSingle(t *testing.T) {
	assert := assert.New(t)

	items := Sequence.Ints(10)
	var counter int32

	// trivial test, counter should equal 10.
	Parallel.Each(items, 1, func(_ interface{}) {
		atomic.AddInt32(&counter, 1)
	})
	assert.Equal(counter, len(items))
}

func TestParallelEachOverdetermined(t *testing.T) {
	assert := assert.New(t)

	items := Sequence.Ints(5)
	var counter int32

	// here we test that the effective parallelism is 5.
	Parallel.Each(items, 10, func(_ interface{}) {
		atomic.AddInt32(&counter, 1)
	})
	assert.Equal(counter, len(items))
}
