package event

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/app/workerpool"
	"github.com/stretchr/testify/assert"
)

type execPool struct {
	workers *workerpool.WorkerPool
}

func (pool *execPool) Exec(task func()) {
	pool.workers.Submit(task)
}

func (pool *execPool) PoolSize() int {
	return pool.workers.Size()
}

func (pool *execPool) QueueSize() int {
	return pool.workers.WaitingQueueSize()
}

func TestNewEventBus(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	if bus == nil {
		t.Log("NewEventBus EventBus not created!")
		t.Fail()
	}
}

type testInterface1 interface {
	test1() bool
}

type testInterface2 interface {
	test2() int
}

type testImpl struct {
}

func (t *testImpl) test1() bool { return true }
func (t *testImpl) test2() int  { return 10 }

func TestHasCallback(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	err := bus.Subscribe(func() {})

	assert.Nil(t, err, "Subscribe failed")

	if bus.HasCallback(reflect.TypeOf("String")) {
		t.Fail()
	}
	if !bus.HasCallback() {
		t.Fail()
	}
}

func TestSubscribe(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	if bus.Subscribe(func() {}) != nil {
		t.Fail()
	}
	if bus.Subscribe("String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnce(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	if bus.SubscribeOnce(func() {}) != nil {
		t.Fail()
	}
	if bus.SubscribeOnce("String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnceAndManySubscribe(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	flag := 0
	fn0 := func() { flag++ }
	fn1 := func() { flag++ }
	fn2 := func() { flag++ }

	err := bus.SubscribeOnce(fn0)
	assert.Nil(t, err, "SubscribeOnce failed")

	err = bus.Subscribe(fn1)
	assert.Nil(t, err, "Subscribe failed")

	err = bus.Subscribe(fn2)
	assert.Nil(t, err, "Subscribe failed")

	bus.Publish()

	if flag != 3 {
		t.Fail()
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	handler := func() {}
	err := bus.Subscribe(handler)
	assert.Nil(t, err, "Subscribe failed")

	if bus.Unsubscribe(handler) != nil {
		t.Fail()
	}
	if bus.Unsubscribe(handler) == nil {
		t.Fail()
	}
}

func TestPublish(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	handler := func(a int, b int) {
		if a != b {
			t.Fail()
		}
	}
	err := bus.Subscribe(handler)
	assert.Nil(t, err, "Subscribe failed")

	bus.Publish(10, 10)
	if bus.Unsubscribe(handler) != nil {
		t.Fail()
	}
	if bus.Unsubscribe(handler) == nil {
		t.Fail()
	}
}

func TestSubcribeOnceAsync(t *testing.T) {
	results := make([]int, 0)

	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	err := bus.SubscribeOnceAsync(func(a int, out *[]int) {
		*out = append(*out, a)
	})
	assert.Nil(t, err, "SubscribeOnceAsync failed")

	bus.Publish(10, &results)
	bus.Publish(10, &results)

	bus.WaitAsync()

	if len(results) != 1 {
		t.Fail()
	}

	var cb func(a int, out *[]int)

	if bus.HasCallback(reflect.TypeOf(cb)) {
		t.Fail()
	}
}

func TestSubscribeAsyncTransactional(t *testing.T) {
	results := []int{0, 0}
	numResults := 0
	resultsMutex := sync.Mutex{}

	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	err := bus.SubscribeAsync(func(a int, out *[]int, dur string) {
		sleep, _ := time.ParseDuration(dur)
		time.Sleep(sleep)
		resultsMutex.Lock()
		(*out)[a-1] = a
		numResults++
		resultsMutex.Unlock()
	})
	assert.Nil(t, err, "SubscribeAsync failed")
	bus.Publish(1, &results, "1s")
	bus.Publish(2, &results, "0s")

	bus.WaitAsync()

	assert.Equal(t, 2, numResults, "result count mismatch")
	assert.Equal(t, 1, results[0], "results[0]")
	assert.Equal(t, 2, results[1], "results[1]")
}

func TestSubscribeAsync(t *testing.T) {
	results := make(chan int)

	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	err := bus.SubscribeAsync(func(a int, out chan<- int) {
		out <- a
	})
	assert.Nil(t, err, "SubscribeAsync failed")

	bus.Publish(1, chan<- int(results))
	bus.Publish(2, chan<- int(results))

	numResults := 0
	resultsMutex := sync.Mutex{}
	go func() {
		for range results {
			resultsMutex.Lock()
			numResults++
			resultsMutex.Unlock()
		}
	}()

	bus.WaitAsync()

	time.Sleep(10 * time.Millisecond)

	resultsMutex.Lock()
	assert.Equal(t, numResults, 2, "numResults count unexpected")
	resultsMutex.Unlock()
}

func TestInterfaceSubscribe(t *testing.T) {
	bus := NewEventBus(&execPool{workerpool.New(runtime.NumCPU())})
	var i int
	var b bool

	if err := bus.Subscribe(func(i1 testInterface1) { b = i1.test1() }); err != nil {
		t.Fatal(err)
	}
	if err := bus.Subscribe(func(i2 testInterface2) { i = i2.test2() }); err != nil {
		t.Fatal(err)
	}

	ti := &testImpl{}

	bus.Publish(ti)

	if !b {
		t.Fatal(fmt.Sprintf("b expected: %v, got: %v", true, b))
	}

	if i != 10 {
		t.Fatal(fmt.Sprintf("i expected: %d, got: %d", 10, i))
	}
}
