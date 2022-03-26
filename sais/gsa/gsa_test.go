package gsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func ExampleGSA() {
	R := [][]byte{[]byte("hihihi")}
	str, n := ConcatAll(R)
	sa2 := make([]uint, SaSize(n))
	lcp := make([]int, LcpSize(n))
	_ = GSA(str, sa2, lcp, nil)
	for i := 0; i < n; i++ {
		j := sa2[i]
		for ; int(j) < n; j++ {
			if str[j] == 1 {
				fmt.Printf("$")
				break
			} else if str[j] == 0 {
				fmt.Printf("#")
			} else {
				fmt.Printf("%c", str[j]-1)
			}
		}
		fmt.Printf("\n")
	}
	fmt.Printf("%d\n", sa2)
}

func TestGSA(t *testing.T) {
	R := [][]byte{{4, 5, 6, 4, 5, 6, 4, 5, 6}}
	str, n := ConcatAll(R)
	sa := make([]uint, SaSize(n))
	lcp := make([]int, n)
	_ = GSA(str, sa, lcp, nil)
	assert.Equal(t, []uint{10, 9, 6, 3, 0, 7, 4, 1, 8, 5, 2}, sa[:n])
}
