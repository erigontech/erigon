package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/valyala/fastjson"
)

func Replay(tgURL string, recordFile string) {
	setRoutes(tgURL, "")
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	f, err := os.Open(recordFile)
	if err != nil {
		fmt.Printf("Cannot open file %s for replay: %v\n", recordFile, err)
		return
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var buf [64 * 1024 * 1024]byte // 64 Mb line buffer
	s.Buffer(buf[:], len(buf))
	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}
	for s.Scan() {
		// Request comes firs
		request := s.Text()
		res = reqGen.TurboGeth2("", request)
		if res.Err != nil {
			fmt.Printf("Could not get replay for %s: %v\n", request, res.Err)
			return
		}
		if errVal := res.Result.Get("error"); errVal != nil {
			fmt.Printf("Error getting replay for %s: %d %s\n", request, errVal.GetInt("code"), errVal.GetStringBytes("message"))
			return
		}
		s.Scan() // Advance to the expected response
		expectedResult, err1 := fastjson.ParseBytes(s.Bytes())
		if err1 != nil {
			fmt.Printf("Could not parse expected result %s: %v\n", request, err1)
			return
		}
		if err := compareResults(res.Result, expectedResult); err != nil {
			fmt.Printf("Different results for %s\n", request)
			fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
			fmt.Printf("\n\nG response=================================\n%s\n", s.Bytes())
			return
		}
		s.Scan()
		s.Scan() // Skip the extra new line between response and the next request
	}
}
