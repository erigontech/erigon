package requests

import "fmt"

func MockGetRequest(reqId int) error {
	reqGen := initialiseRequestGenerator(reqId)
	res := reqGen.Get()
	if res.Err != nil {
		return fmt.Errorf("failed to make get request: %v", res.Err)
	}
	fmt.Printf("SUCCESS => OK\n")
	return nil
}
