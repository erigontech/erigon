package metrics

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"
)

// Stop the compiler from complaining during debugging.
var (
	_ = ioutil.Discard
	_ = log.LstdFlags
)

func Example() {
	c := NewCounter()
	Register("money", c)
	c.Inc(17)

	// Threadsafe registration
	t := GetOrRegisterTimer("db.get.latency", nil)
	t.Time(func() { time.Sleep(10 * time.Millisecond) })
	t.Update(1)

	fmt.Println(c.Count())
	fmt.Println(t.Min())
	// Output: 17
	// 1
}
