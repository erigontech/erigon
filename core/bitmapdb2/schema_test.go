package bitmapdb2

import (
	"math/rand"
	"testing"
)

func TestContainer(t *testing.T) {
	hi := 14
	base := uint32(hi) << 16
	c := NewEmptyContainer(uint16(hi))

	seed := int64(999777700)
	r := rand.New(rand.NewSource(seed))
	var numbers []uint16
	numberMap := make(map[uint16]struct{})
	for i := 0; i < 8000; i++ {
		v := r.Intn(1 << 16)
		numbers = append(numbers, uint16(v))
		numberMap[uint16(v)] = struct{}{}
	}

	var numbersNonExist []uint16
	for i := 0; i < 65536; i++ {
		if _, ok := numberMap[uint16(i)]; !ok {
			numbersNonExist = append(numbersNonExist, uint16(i))
		}
	}

	for i, v := range numbers {
		c.Add(base + uint32(v))

		for j := 0; j <= i; j++ {
			n := base + uint32(numbers[j])
			if !c.Contains(n) {
				t.Fatalf("container not contains %d", n)
			}
		}
		for j := 0; j < len(numbersNonExist); j++ {
			n := base + uint32(numbersNonExist[j])
			if c.Contains(n) {
				t.Fatalf("container contains %d", n)
			}
		}
	}
}
