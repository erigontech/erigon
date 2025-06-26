package txpool

import (
	"encoding/json"
	"io"
	"os"

	"github.com/erigontech/erigon-lib/common"
)

// priorityListJSON represents the JSON structure with senders array
type priorityListJSON struct {
	Senders []string `json:"senders"`
}

// UnmarshalDynamicPriorityList reads a JSON file of accounts from the given path.
func UnmarshalDynamicPriorityList(path string) (*PriorityList, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	var jsonData priorityListJSON
	if err = json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	priorityList := &PriorityList{
		addresses: make(map[common.Address]uint64),
	}

	for i, addrStr := range jsonData.Senders {
		addr := common.HexToAddress(addrStr)
		priority := uint64(len(jsonData.Senders) - i)
		priorityList.addresses[addr] = priority
	}

	return priorityList, nil
}

type Priority uint64

const (
	NoPriority Priority = 0
)

type PriorityList struct {
	addresses map[common.Address]uint64
}

func (p *PriorityList) GetPriority(addr common.Address) Priority {
	if priority, exists := p.addresses[addr]; exists {
		return Priority(priority)
	}
	return NoPriority
}

func (p *PriorityList) Addresses() []common.Address {
	if p.addresses == nil {
		return []common.Address{}
	}

	addresses := make([]common.Address, 0, len(p.addresses))
	for addr := range p.addresses {
		addresses = append(addresses, addr)
	}

	return addresses
}
