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

package rpctest

import (
	"bufio"
	"fmt"
	"os"

	"github.com/valyala/fastjson"
)

func Replay(erigonURL string, recordFile string) error {
	setRoutes(erigonURL, "")
	f, err := os.Open(recordFile)
	if err != nil {
		fmt.Printf("Cannot open file %s for replay: %v\n", recordFile, err)
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var buf [64 * 1024 * 1024]byte // 64 Mb line buffer
	s.Buffer(buf[:], len(buf))
	var res CallResult
	reqGen := &RequestGenerator{}
	for s.Scan() {
		// Request comes firs
		request := s.Text()
		res = reqGen.Erigon2("", request)
		if res.Err != nil {
			return fmt.Errorf("could not get replay for %s: %w", request, res.Err)
		}
		if errVal := res.Result.Get("error"); errVal != nil {
			return fmt.Errorf("error getting replay for %s: %d %s", request, errVal.GetInt("code"), errVal.GetStringBytes("message"))
		}
		s.Scan() // Advance to the expected response
		expectedResult, err1 := fastjson.ParseBytes(s.Bytes())
		if err1 != nil {
			return fmt.Errorf("could not parse expected result %s: %w", request, err1)
		}
		if err := compareResults(res.Result, expectedResult); err != nil {
			fmt.Printf("Different results for %s:\n %v\n", request, err)
			fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
			fmt.Printf("\n\nG response=================================\n%s\n", s.Bytes())
			return fmt.Errorf("different results for %s:\n %w", request, err)
		}
		s.Scan()
		s.Scan() // Skip the extra new line between response and the next request
	}
	return nil
}
