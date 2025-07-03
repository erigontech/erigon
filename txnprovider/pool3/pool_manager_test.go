package pool3

import "testing"

func TestPoolManager(t *testing.T) {
	var num uint64
	num = 25
	pooler := num - 100 
	t.Error(pooler)
}