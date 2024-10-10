package requests

import "fmt"

func PingErigonRpc(reqId int) error {
	reqGen := initialiseRequestGenerator(reqId)
	res := reqGen.PingErigonRpc()
	if res.Err != nil {
		return fmt.Errorf("failed to ping erigon rpc url: %v", res.Err)
	}
	fmt.Printf("SUCCESS => OK\n")
	return nil
}
