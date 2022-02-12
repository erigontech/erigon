package requests

import "fmt"

func MockGetRequest(reqId int) {
	reqGen := initialiseRequestGenerator(reqId)

	res := reqGen.Get()

	if res.Err != nil {
		fmt.Printf("error: %v\n", res.Err)
		return
	}

	fmt.Printf("OK\n")
}
