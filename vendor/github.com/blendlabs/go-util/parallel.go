package util

import (
	"fmt"
	"reflect"
	"sync"
)

var (
	// Parallel contains parallel utils.
	Parallel = parallelUtil{}
)

type parallelUtil struct{}

func (pu parallelUtil) Each(collection interface{}, parallelism int, action func(interface{})) error {
	t := reflect.TypeOf(collection)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	v := reflect.ValueOf(collection)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if t.Kind() != reflect.Slice {
		return fmt.Errorf("cannot iterate over non-slice")
	}

	effectiveParallelism := parallelism
	if parallelism > v.Len() {
		effectiveParallelism = v.Len()
	}
	if effectiveParallelism < 1 {
		effectiveParallelism = 1
	}

	// edge case, drop into single iterator, do not allocate any
	// goroutines.
	if effectiveParallelism == 1 {
		for i := 0; i < v.Len(); i++ {
			action(v.Index(i).Interface())
		}
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(effectiveParallelism)

	for threadID := 0; threadID < effectiveParallelism; threadID++ {
		go func(startIndex int) {
			defer wg.Done()
			pu.parallelIterator(v, startIndex, effectiveParallelism, action)
		}(threadID)
	}

	wg.Wait()
	return nil
}

func (pu parallelUtil) parallelIterator(collectionValue reflect.Value, startIndex, parallelism int, action func(interface{})) {
	for i := startIndex; i < collectionValue.Len(); i += parallelism {
		action(collectionValue.Index(i).Interface())
	}
}

// Await waits for all actions to complete.
func (pu parallelUtil) Await(actions ...func()) {
	wg := sync.WaitGroup{}
	wg.Add(len(actions))
	for i := 0; i < len(actions); i++ {
		action := actions[i]
		go func() {
			action()
			wg.Done()
		}()
	}

	wg.Wait()
}
